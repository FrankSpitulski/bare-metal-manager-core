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

//! The DPF operator manages all provisioning logic. Carbide's role is:
//! 1. Declare setup (register devices + node)
//! 2. Wait for watcher callbacks (DPU ready, reboot required)
//! 3. Handle cleanup on error/reprovisioning

use std::str::FromStr;

use carbide_dpf::dpu_node_name;
use carbide_uuid::machine::MachineId;
use futures_util::stream::{FuturesUnordered, TryStreamExt};
use health_report::{
    HealthAlertClassification, HealthProbeAlert, HealthProbeId, HealthReport, OverrideMode,
};
use libredfish::SystemPowerControl;
use model::machine::{
    DpfState, DpuInitState, FailureCause, FailureDetails, FailureSource, InstanceState, Machine,
    MachineState, ManagedHostState, ManagedHostStateSnapshot, ReprovisionState, StateMachineArea,
};
use sqlx::PgConnection;

use super::helpers::{DpuInitStateHelper, ReprovisionStateHelper};
use super::{handler_host_power_control, rebooted};
use crate::dpf::DpfOperations;
use crate::state_controller::machine::context::MachineStateHandlerContextObjects;
use crate::state_controller::state_handler::{
    StateHandlerContext, StateHandlerError, StateHandlerOutcome,
};

const DPF_MAINTENANCE_SOURCE: &str = "dpf-provisioning";

fn bmc_ip(machine: &Machine) -> Result<&str, StateHandlerError> {
    machine.bmc_info.ip.as_deref().ok_or_else(|| {
        StateHandlerError::GenericError(eyre::eyre!("BMC IP is not set for machine {}", machine.id))
    })
}

/// Check whether the DPF provisioning maintenance override is set on the host.
fn is_in_dpf_maintenance(state: &ManagedHostStateSnapshot) -> bool {
    state
        .host_snapshot
        .health_report_overrides
        .merges
        .contains_key(DPF_MAINTENANCE_SOURCE)
}

/// Set a health override that prevents new allocations while DPF provisioning
/// performs disruptive operations on the host.
async fn enter_dpf_maintenance(
    state: &ManagedHostStateSnapshot,
    txn: &mut PgConnection,
) -> Result<(), StateHandlerError> {
    let host_id = &state.host_snapshot.id;
    let report = HealthReport {
        source: DPF_MAINTENANCE_SOURCE.to_string(),
        observed_at: Some(chrono::Utc::now()),
        alerts: vec![HealthProbeAlert {
            id: HealthProbeId::from_str("DpfProvisioning").expect("valid probe id"),
            target: None,
            in_alert_since: Some(chrono::Utc::now()),
            message: "DPF provisioning requires host maintenance".to_string(),
            tenant_message: None,
            classifications: vec![HealthAlertClassification::prevent_allocations()],
        }],
        successes: vec![],
    };
    db::machine::insert_health_report_override(txn, host_id, OverrideMode::Merge, &report, false)
        .await?;
    tracing::info!(%host_id, "Host entered DPF maintenance");
    Ok(())
}

/// Clear the DPF provisioning maintenance override.
async fn exit_dpf_maintenance(
    state: &ManagedHostStateSnapshot,
    txn: &mut PgConnection,
) -> Result<(), StateHandlerError> {
    let host_id = &state.host_snapshot.id;
    db::machine::remove_health_report_override(
        txn,
        host_id,
        OverrideMode::Merge,
        DPF_MAINTENANCE_SOURCE,
    )
    .await
    .ok(); // Ignore error if override doesn't exist
    tracing::info!(%host_id, "Host exited DPF maintenance");
    Ok(())
}

/// Transition all DPU sub-states to the given DPF state, preserving the
/// outer managed-host state (`DPUInit` or `DPUReprovision`).
fn transition_all_dpus_to_dpf_state(
    next_dpf: DpfState,
    state: &ManagedHostStateSnapshot,
) -> Result<ManagedHostState, StateHandlerError> {
    match &state.managed_state {
        ManagedHostState::DPUInit { .. } | ManagedHostState::DpuDiscoveringState { .. } => {
            DpuInitState::DpfStates { state: next_dpf }
                .next_state_with_all_dpus_updated(&state.managed_state)
        }
        ManagedHostState::DPUReprovision { .. }
        | ManagedHostState::Assigned {
            instance_state: InstanceState::DPUReprovision { .. },
        } => {
            let all_dpu_ids = state.dpu_snapshots.iter().map(|x| &x.id).collect();
            ReprovisionState::DpfStates { substate: next_dpf }.next_state_with_all_dpus_updated(
                &state.managed_state,
                &state.dpu_snapshots,
                all_dpu_ids,
            )
        }
        other => Err(StateHandlerError::InvalidState(format!(
            "Cannot transition DPF sub-states in {other:?}"
        ))),
    }
}

/// Update a single DPU's DPF sub-state. All other DPUs are unchanged.
/// Use when persisting a phase change or moving one DPU to the next DpfState.
fn set_one_dpu_dpf_state(
    state: &ManagedHostStateSnapshot,
    dpu_id: &MachineId,
    next_dpf: DpfState,
) -> Result<ManagedHostState, StateHandlerError> {
    let mut next_state = state.managed_state.clone();
    match &mut next_state {
        ManagedHostState::DPUInit { dpu_states } => {
            dpu_states
                .states
                .insert(*dpu_id, DpuInitState::DpfStates { state: next_dpf });
        }
        ManagedHostState::DPUReprovision { dpu_states } => {
            dpu_states
                .states
                .insert(*dpu_id, ReprovisionState::DpfStates { substate: next_dpf });
        }
        ManagedHostState::Assigned {
            instance_state: InstanceState::DPUReprovision { dpu_states },
        } => {
            dpu_states
                .states
                .insert(*dpu_id, ReprovisionState::DpfStates { substate: next_dpf });
        }
        other => {
            return Err(StateHandlerError::InvalidState(format!(
                "Cannot set DPF state for one DPU in {other:?}"
            )));
        }
    }
    Ok(next_state)
}

/// Determine the correct next state when exiting `WaitingForReady`, based on
/// whether we are in initial provisioning (`DPUInit`) or reprovisioning
/// (`DPUReprovision`).
fn waiting_for_ready_exit_state(
    state: &ManagedHostStateSnapshot,
) -> Result<ManagedHostState, StateHandlerError> {
    match &state.managed_state {
        ManagedHostState::DPUInit { .. } | ManagedHostState::DpuDiscoveringState { .. } => {
            Ok(ManagedHostState::HostInit {
                machine_state: MachineState::EnableIpmiOverLan,
            })
        }
        ManagedHostState::DPUReprovision { .. }
        | ManagedHostState::Assigned {
            instance_state: InstanceState::DPUReprovision { .. },
        } => {
            let all_dpu_ids = state.dpu_snapshots.iter().map(|x| &x.id).collect();
            ReprovisionState::PoweringOffHost.next_state_with_all_dpus_updated(
                &state.managed_state,
                &state.dpu_snapshots,
                all_dpu_ids,
            )
        }
        other => Err(StateHandlerError::InvalidState(format!(
            "Cannot exit DPF WaitingForReady in {other:?}"
        ))),
    }
}

/// Handle DPF state transitions.
///
/// SDK operations are applied to **all** DPUs on the host so that multi-DPU
/// hosts have every device registered / reprovisioned before the bulk state
/// transition fires. The caller's per-DPU loop will see the returned
/// `Transition` on the first DPU and break, which is correct because we have
/// already handled all DPUs here.
#[allow(txn_held_across_await)]
pub async fn handle_dpf_state(
    state: &ManagedHostStateSnapshot,
    dpu_snapshot: &Machine,
    dpf_state: &DpfState,
    txn: &mut PgConnection,
    ctx: &mut StateHandlerContext<'_, MachineStateHandlerContextObjects>,
    dpf_sdk: &dyn DpfOperations,
) -> Result<StateHandlerOutcome<ManagedHostState>, StateHandlerError> {
    match dpf_state {
        DpfState::Provisioning => {
            // Register every DPU device on the host.
            for dpu in &state.dpu_snapshots {
                let serial_number = dpu
                    .hardware_info
                    .as_ref()
                    .and_then(|x| x.dmi_data.as_ref())
                    .map(|x| x.product_serial.as_str())
                    .unwrap_or_default();
                let device_info = carbide_dpf::DpuDeviceInfo {
                    device_name: dpu.id.to_string(),
                    dpu_bmc_ip: bmc_ip(dpu)?.to_string(),
                    host_bmc_ip: bmc_ip(&state.host_snapshot)?.to_string(),
                    serial_number: serial_number.to_string(),
                };
                dpf_sdk.register_dpu_device(device_info).await?;
            }

            // Register the DPU node referencing all devices.
            let dpu_ids: Vec<String> = state
                .dpu_snapshots
                .iter()
                .map(|d| d.id.to_string())
                .collect();
            let node_info = carbide_dpf::DpuNodeInfo {
                node_id: state.host_snapshot.id.to_string(),
                host_bmc_ip: bmc_ip(&state.host_snapshot)?.to_string(),
                dpu_device_names: dpu_ids,
            };
            dpf_sdk.register_dpu_node(node_info).await?;

            let next =
                transition_all_dpus_to_dpf_state(DpfState::WaitingForReady { phase: None }, state)?;
            Ok(StateHandlerOutcome::transition(next))
        }
        DpfState::WaitingForReady { phase } => {
            let node_name = dpu_node_name(&state.host_snapshot.id.to_string());
            let device_name = dpu_snapshot.id.to_string();
            let current_phase = dpf_sdk.get_dpu_phase(&device_name, &node_name).await?;
            let phase_changed = phase.as_deref() != Some(current_phase.as_ref());

            // Commit the maintenance override before performing disruptive
            // operations. The periodic enqueuer will re-trigger this handler
            // on the next cycle, at which point the host is in maintenance
            // and we proceed to release holds / reboot.
            // TODO: DPUNodeMaintenance.spec.node_effect.drain may require
            // draining instances from the host before proceeding.
            if !is_in_dpf_maintenance(state) {
                enter_dpf_maintenance(state, txn).await?;
                // keep the informational phase string updated if it changed.
                if phase_changed {
                    let updated = set_one_dpu_dpf_state(
                        state,
                        &dpu_snapshot.id,
                        DpfState::WaitingForReady {
                            phase: Some(current_phase.to_string()),
                        },
                    )?;
                    return Ok(StateHandlerOutcome::transition(updated));
                }
                return Ok(StateHandlerOutcome::wait(
                    "Entered DPF maintenance, waiting for next cycle to proceed".to_string(),
                ));
            }

            // Release maintenance hold so the DPF operator can proceed past NodeEffect.
            // Idempotent: safe to call if already released.
            dpf_sdk.release_maintenance_hold(&node_name).await?;

            // Handle host reboot when the DPF operator requires one.
            // Only clear the annotation once scout confirms the OS is up.
            if dpf_sdk.is_reboot_required(&node_name).await? {
                if rebooted(&state.host_snapshot) {
                    dpf_sdk.reboot_complete(&node_name).await?;
                } else {
                    // don't send multiple reboot requests for the same host.
                    let already_requested = state
                        .host_snapshot
                        .last_reboot_requested
                        .as_ref()
                        .is_some_and(|r| r.time > state.host_snapshot.state.version.timestamp());
                    if !already_requested {
                        // send the reboot request to the host. (still need to wait for reboot to complete)
                        handler_host_power_control(
                            state,
                            ctx.services,
                            SystemPowerControl::ForceRestart,
                            txn,
                        )
                        .await?;
                    }
                    // keep the informational phase string updated if it changed.
                    if phase_changed {
                        let updated = set_one_dpu_dpf_state(
                            state,
                            &dpu_snapshot.id,
                            DpfState::WaitingForReady {
                                phase: Some(current_phase.to_string()),
                            },
                        )?;
                        return Ok(StateHandlerOutcome::transition(updated));
                    }
                    return Ok(StateHandlerOutcome::wait(
                        "Waiting for host to come back after DPF reboot".to_string(),
                    ));
                }
            }

            if current_phase == carbide_dpf::DpuPhase::Error {
                tracing::error!(
                    host = %state.host_snapshot.id,
                    dpu = %dpu_snapshot.id,
                    "DPU entered error phase during DPF provisioning"
                );
                exit_dpf_maintenance(state, txn).await?;
                return Ok(StateHandlerOutcome::transition(ManagedHostState::Failed {
                    details: FailureDetails {
                        cause: FailureCause::DpfProvisioning {
                            err: format!(
                                "DPU {} entered error phase during DPF provisioning",
                                dpu_snapshot.id
                            ),
                        },
                        failed_at: chrono::Utc::now(),
                        source: FailureSource::StateMachineArea(StateMachineArea::MainFlow),
                    },
                    machine_id: dpu_snapshot.id,
                    retry_count: 0,
                }));
            }
            if !all_dpus_ready(&state.dpu_snapshots, dpf_sdk).await? {
                if phase_changed {
                    let updated = set_one_dpu_dpf_state(
                        state,
                        &dpu_snapshot.id,
                        DpfState::WaitingForReady {
                            phase: Some(current_phase.to_string()),
                        },
                    )?;
                    return Ok(StateHandlerOutcome::transition(updated));
                }
                return Ok(StateHandlerOutcome::wait(
                    "Waiting for all DPUs to report device ready before exiting DPF state"
                        .to_string(),
                ));
            }

            exit_dpf_maintenance(state, txn).await?;
            let next = waiting_for_ready_exit_state(state)?;
            Ok(StateHandlerOutcome::transition(next))
        }
        DpfState::Reprovisioning => {
            let node_name = dpu_node_name(&state.host_snapshot.id.to_string());
            dpf_sdk
                .reprovision_dpu(&dpu_snapshot.id.to_string(), &node_name)
                .await?;
            let next = set_one_dpu_dpf_state(
                state,
                &dpu_snapshot.id,
                DpfState::WaitingForReady { phase: None },
            )?;
            Ok(StateHandlerOutcome::transition(next))
        }
    }
}

async fn all_dpus_ready(
    dpus: &[Machine],
    dpf_sdk: &dyn DpfOperations,
) -> Result<bool, StateHandlerError> {
    FuturesUnordered::from_iter(dpus.iter().map(|dpu| {
        let dpu_id = dpu.id.to_string();
        async move {
            dpf_sdk
                .is_dpu_device_ready(&dpu_id)
                .await
                .map_err(Into::into)
        }
    }))
    .try_fold(true, |acc, r| std::future::ready(Ok(acc && r)))
    .await
}
