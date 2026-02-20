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

// Provisioning CRDs (provisioning.dpu.nvidia.com)
pub mod bfbs_generated;
pub mod dpuclusters_generated;
pub mod dpudevices_generated;
pub mod dpudiscoveries_generated;
pub mod dpuflavors_generated;
pub mod dpunodemaintenances_generated;
pub mod dpunodes_generated;
pub mod dpus_generated;
pub mod dpusets_generated;

// Service CRDs (svc.dpu.nvidia.com)
pub mod dpudeployments_generated;
pub mod dpuservicechains_generated;
pub mod dpuserviceconfigurations_generated;
pub mod dpuservicecredentialrequests_generated;
pub mod dpuserviceinterfaces_generated;
pub mod dpuserviceipams_generated;
pub mod dpuservicenads_generated;
pub mod dpuservices_generated;
pub mod dpuservicetemplates_generated;
pub mod servicechains_generated;
pub mod servicechainsets_generated;
pub mod serviceinterfaces_generated;
pub mod serviceinterfacesets_generated;

// Operator CRDs (operator.dpu.nvidia.com)
pub mod dpfoperatorconfigs_generated;

// Storage CRDs (storage.dpu.nvidia.com)
pub mod dpustoragepolicies_generated;
pub mod dpustoragevendors_generated;
pub mod dpuvolumeattachments_generated;
pub mod dpuvolumes_generated;

// VPC CRDs (vpc.dpu.nvidia.com)
pub mod dpuvirtualnetworks_generated;
pub mod dpuvpcs_generated;
pub mod isolationclasses_generated;
