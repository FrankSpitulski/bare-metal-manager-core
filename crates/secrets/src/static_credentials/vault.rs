/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;

use super::{StaticCredentialKey, StaticCredentialReader};
use crate::SecretsError;
use crate::credentials::{CredentialType, Credentials, SecretPathReader};

pub struct VaultBackedStaticCredentialReader {
    inner: Arc<dyn SecretPathReader>,
}

impl VaultBackedStaticCredentialReader {
    pub fn new(inner: Arc<dyn SecretPathReader>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl StaticCredentialReader for VaultBackedStaticCredentialReader {
    async fn get_credentials(
        &self,
        key: &StaticCredentialKey,
    ) -> Result<Option<Credentials>, SecretsError> {
        self.inner
            .get_credentials_by_path(vault_path(key).as_ref())
            .await
    }
}

fn vault_path(key: &StaticCredentialKey) -> Cow<'_, str> {
    match key {
        StaticCredentialKey::DpuRedfish { credential_type } => match credential_type {
            CredentialType::DpuHardwareDefault => {
                Cow::from("machines/all_dpus/factory_default/bmc-metadata-items/root")
            }
            CredentialType::SiteDefault => {
                Cow::from("machines/all_dpus/site_default/bmc-metadata-items/root")
            }
            CredentialType::HostHardwareDefault { .. } => {
                unreachable!(
                    "DpuRedfish / HostHardwareDefault is an invalid credential combination"
                );
            }
        },
        StaticCredentialKey::HostRedfish { credential_type } => match credential_type {
            CredentialType::HostHardwareDefault { vendor } => Cow::from(format!(
                "machines/all_hosts/factory_default/bmc-metadata-items/{vendor}"
            )),
            CredentialType::SiteDefault => {
                Cow::from("machines/all_hosts/site_default/bmc-metadata-items/root")
            }
            CredentialType::DpuHardwareDefault => {
                unreachable!(
                    "HostRedfish / DpuHardwareDefault is an invalid credential combination"
                );
            }
        },
        StaticCredentialKey::UfmAuth { fabric } => Cow::from(format!("ufm/{fabric}/auth")),
        StaticCredentialKey::DpuUefi { credential_type } => match credential_type {
            CredentialType::DpuHardwareDefault => {
                Cow::from("machines/all_dpus/factory_default/uefi-metadata-items/auth")
            }
            CredentialType::SiteDefault => {
                Cow::from("machines/all_dpus/site_default/uefi-metadata-items/auth")
            }
            CredentialType::HostHardwareDefault { .. } => {
                unreachable!("DpuUefi / HostHardwareDefault is an invalid credential combination");
            }
        },
        StaticCredentialKey::HostUefi { credential_type } => match credential_type {
            CredentialType::SiteDefault => {
                Cow::from("machines/all_hosts/site_default/uefi-metadata-items/auth")
            }
            _ => {
                unreachable!("HostUefi only supports SiteDefault");
            }
        },
        StaticCredentialKey::NmxM { nmxm_id } => Cow::from(format!("nmxm/{nmxm_id}/auth")),
    }
}
