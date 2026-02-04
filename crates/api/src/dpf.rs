/*
 * SPDX-FileCopyrightText: Copyright (c) 2021-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! DPF SDK trait abstraction for testability.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use carbide_dpf::{
    DpfError, DpfInitConfig, DpfSdk, DpuDeviceInfo, DpuNodeInfo, DpuPhase, DpuWatcher,
    KubeRepository, ResourceLabeler, node_id_from_node_name,
};
use carbide_uuid::machine::MachineId;
use sqlx::PgPool;

use crate::state_controller::controller::Enqueuer;
use crate::state_controller::machine::io::MachineStateControllerIO;

/// Trait for DPF SDK operations used by Carbide.
///
/// The DPF operator owns provisioning; Carbide declares setup (deployment, devices, node),
/// reacts to watcher callbacks, and performs reprovision/force-delete.
///
/// Reboot handling is managed via the watcher's `on_reboot_required` callback.
#[allow(dead_code)] // we're mocking the entire dpf sdk, even if we don't use all methods yet.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DpfOperations: Send + Sync + std::fmt::Debug {
    /// Create initialization objects (BFB, DPUFlavor, DPUDeployment with DPUServiceTemplate/DPUServiceConfiguration).
    async fn create_initialization_objects(&self, config: &DpfInitConfig) -> Result<(), DpfError>;

    /// Register a DPU device.
    async fn register_dpu_device(&self, info: DpuDeviceInfo) -> Result<(), DpfError>;

    /// Check if a DPU device is ready.
    async fn is_dpu_device_ready(&self, dpu_device_name: &str) -> Result<bool, DpfError>;

    /// Register a DPU node.
    async fn register_dpu_node(&self, info: DpuNodeInfo) -> Result<(), DpfError>;

    /// Release the maintenance hold on a DPU node.
    async fn release_maintenance_hold(&self, node_name: &str) -> Result<(), DpfError>;

    /// Reprovision a DPU (delete DPU CR; operator creates a new one that waits on node effect).
    async fn reprovision_dpu(&self, dpu_device_name: &str, node_name: &str)
    -> Result<(), DpfError>;

    /// Force delete a host and all its DPU resources.
    async fn force_delete_host(
        &self,
        node_name: &str,
        dpu_device_names: &[String],
    ) -> Result<(), DpfError>;

    /// Get the current phase of a DPU (for status reporting).
    async fn get_dpu_phase(
        &self,
        dpu_device_name: &str,
        node_name: &str,
    ) -> Result<DpuPhase, DpfError>;

    /// Update the BFB reference in a DPUDeployment (for BFB upgrade).
    async fn update_deployment_bfb(
        &self,
        deployment_name: &str,
        bfb_name: &str,
    ) -> Result<(), DpfError>;

    /// Force delete a single DPU and its device (e.g. when removing one DPU from a host).
    async fn force_delete_dpu(
        &self,
        dpu_device_name: &str,
        node_name: &str,
    ) -> Result<(), DpfError>;

    /// Check if a DPU node is waiting for external reboot.
    async fn is_reboot_required(&self, node_name: &str) -> Result<bool, DpfError>;

    /// Mark DPU node as rebooted (clear the external reboot required annotation).
    async fn reboot_complete(&self, node_name: &str) -> Result<(), DpfError>;

    /// Delete a DPU device.
    async fn delete_dpu_device(&self, dpu_device_name: &str) -> Result<(), DpfError>;

    /// Delete a DPU node and associated resources.
    async fn delete_dpu_node(&self, node_name: &str) -> Result<(), DpfError>;

    /// Force delete a DPU node and all its DPU devices.
    async fn force_delete_dpu_node(&self, node_name: &str) -> Result<(), DpfError>;
}

/// Applies carbide-specific labels to DPF resources.
pub struct CarbideDPFLabeler;

impl ResourceLabeler for CarbideDPFLabeler {
    fn device_labels(&self, info: &DpuDeviceInfo) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "carbide.nvidia.com/controlled.device".to_string(),
                "true".to_string(),
            ),
            (
                "carbide.nvidia.com/host-bmc-ip".to_string(),
                info.host_bmc_ip.clone(),
            ),
        ])
    }

    fn node_labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(
            "carbide.nvidia.com/controlled.node".to_string(),
            "true".to_string(),
        )])
    }
}

/// DPF SDK operations implementation that wraps the real DPF SDK.
pub struct DpfSdkOps {
    sdk: Arc<DpfSdk<KubeRepository, CarbideDPFLabeler>>,
    _watcher: DpuWatcher,
}

impl DpfSdkOps {
    /// Create a new DpfSdkOps using the DPF SDK and sets up watcher callbacks to trigger carbidestate handling.
    pub fn new(sdk: Arc<DpfSdk<KubeRepository, CarbideDPFLabeler>>, db_pool: PgPool) -> Self {
        let watcher = sdk
            .watcher()
            .on_dpu_event(|event| async move {
                tracing::debug!(
                    dpu = %event.dpu_name,
                    device_name = %event.device_name,
                    node = %event.node_name,
                    phase = ?event.phase,
                    "DPF DPU event"
                );
                Ok(())
            })
            .on_reboot_required({
                let db_pool = db_pool.clone();
                move |event| {
                    let db_pool = db_pool.clone();
                    async move {
                        tracing::info!(
                            node = %event.node_name,
                            host = %event.host_bmc_ip,
                            "DPF reboot required"
                        );
                        enqueue_host(&db_pool, &event.node_name, "reboot").await
                    }
                }
            })
            .on_dpu_ready({
                let db_pool = db_pool.clone();
                move |event| {
                    let db_pool = db_pool.clone();
                    async move {
                        tracing::info!(
                            dpu = %event.dpu_name,
                            device_name = %event.device_name,
                            node = %event.node_name,
                            "DPF DPU ready"
                        );
                        enqueue_host(&db_pool, &event.node_name, "ready").await
                    }
                }
            })
            .on_maintenance_needed({
                let db_pool = db_pool.clone();
                move |event| {
                    let db_pool = db_pool.clone();
                    async move {
                        tracing::info!(
                            node = %event.node_name,
                            "DPF maintenance needed (NodeEffect phase)"
                        );
                        enqueue_host(&db_pool, &event.node_name, "maintenance").await
                    }
                }
            })
            .on_error({
                move |event| {
                    let db_pool = db_pool.clone();
                    async move {
                        tracing::error!(
                            dpu = %event.dpu_name,
                            device_name = %event.device_name,
                            node = %event.node_name,
                            "DPF DPU entered error phase"
                        );
                        enqueue_host(&db_pool, &event.node_name, "error").await
                    }
                }
            })
            .start();

        Self {
            sdk,
            _watcher: watcher,
        }
    }
}

/// Look up a host by DPUNode name and enqueue it for state handling.
/// Node name format: "dpu-node-{host_machine_id}".
#[allow(txn_held_across_await)]
async fn enqueue_host(db_pool: &PgPool, node_name: &str, reason: &str) -> Result<(), DpfError> {
    let node_id = node_id_from_node_name(node_name);

    let host_machine_id = MachineId::from_str(node_id)
        .map_err(|e| DpfError::InvalidState(format!("Invalid node_id: {node_id}: {e}")))?;

    let host = {
        let mut conn = db_pool.acquire().await.map_err(|e| {
            DpfError::InvalidState(format!("Failed to acquire database connection: {e}"))
        })?;
        db::machine::find_one(
            &mut conn,
            &host_machine_id,
            model::machine::machine_search_config::MachineSearchConfig::default(),
        )
        .await
        .map_err(|e| DpfError::InvalidState(format!("DB error looking up host: {e}")))?
    };

    let Some(host) = host else {
        tracing::warn!(node = %node_name, reason, "Could not find host for DPF node");
        return Ok(());
    };

    Enqueuer::<MachineStateControllerIO>::new(db_pool.clone())
        .enqueue_object(&host.id)
        .await
        .map_err(|e| {
            DpfError::InvalidState(format!("Failed to enqueue machine {}: {e}", host.id))
        })?;

    tracing::info!(node = %node_name, host = %host.id, reason, "Enqueued host for DPF state handling");
    Ok(())
}

impl std::fmt::Debug for DpfSdkOps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpfSdkOps").finish()
    }
}

/// Delegates everything to the underlying DPF SDK.
#[async_trait]
impl DpfOperations for DpfSdkOps {
    async fn create_initialization_objects(&self, config: &DpfInitConfig) -> Result<(), DpfError> {
        self.sdk.create_initialization_objects(config).await
    }

    async fn register_dpu_device(&self, info: DpuDeviceInfo) -> Result<(), DpfError> {
        self.sdk.register_dpu_device(info).await
    }

    async fn is_dpu_device_ready(&self, dpu_device_name: &str) -> Result<bool, DpfError> {
        self.sdk.is_dpu_device_ready(dpu_device_name).await
    }

    async fn register_dpu_node(&self, info: DpuNodeInfo) -> Result<(), DpfError> {
        self.sdk.register_dpu_node(info).await
    }

    async fn release_maintenance_hold(&self, node_name: &str) -> Result<(), DpfError> {
        self.sdk.release_maintenance_hold(node_name).await
    }

    async fn force_delete_host(
        &self,
        node_name: &str,
        dpu_device_names: &[String],
    ) -> Result<(), DpfError> {
        self.sdk
            .force_delete_host(node_name, dpu_device_names)
            .await
    }

    async fn reprovision_dpu(
        &self,
        dpu_device_name: &str,
        node_name: &str,
    ) -> Result<(), DpfError> {
        self.sdk.reprovision_dpu(dpu_device_name, node_name).await
    }

    async fn get_dpu_phase(
        &self,
        dpu_device_name: &str,
        node_name: &str,
    ) -> Result<DpuPhase, DpfError> {
        self.sdk.get_dpu_phase(dpu_device_name, node_name).await
    }

    async fn update_deployment_bfb(
        &self,
        deployment_name: &str,
        bfb_name: &str,
    ) -> Result<(), DpfError> {
        self.sdk
            .update_deployment_bfb(deployment_name, bfb_name)
            .await
    }

    async fn force_delete_dpu(
        &self,
        dpu_device_name: &str,
        node_name: &str,
    ) -> Result<(), DpfError> {
        self.sdk.force_delete_dpu(dpu_device_name, node_name).await
    }

    async fn is_reboot_required(&self, node_name: &str) -> Result<bool, DpfError> {
        self.sdk.is_reboot_required(node_name).await
    }

    async fn reboot_complete(&self, node_name: &str) -> Result<(), DpfError> {
        self.sdk.reboot_complete(node_name).await
    }

    async fn delete_dpu_device(&self, dpu_device_name: &str) -> Result<(), DpfError> {
        self.sdk.delete_dpu_device(dpu_device_name).await
    }

    async fn delete_dpu_node(&self, node_name: &str) -> Result<(), DpfError> {
        self.sdk.delete_dpu_node(node_name).await
    }

    async fn force_delete_dpu_node(&self, node_name: &str) -> Result<(), DpfError> {
        self.sdk.force_delete_dpu_node(node_name).await
    }
}
