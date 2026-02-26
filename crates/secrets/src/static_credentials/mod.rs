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
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::SecretsError;
use crate::credentials::{CredentialType, Credentials, SecretPathReader};

mod env;
mod file;
mod vault;

pub use env::{EnvStaticCredentialsConfig, EnvStaticCredentialsProvider};
pub use file::{FileStaticCredentialsConfig, FileStaticCredentialsProvider};
pub use vault::VaultBackedStaticCredentialReader;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct StaticCredentialSnapshot {
    pub dpu_redfish_factory_default: Option<Credentials>,
    pub dpu_redfish_site_default: Option<Credentials>,
    pub host_redfish_factory_default_by_vendor: HashMap<bmc_vendor::BMCVendor, Credentials>,
    pub host_redfish_site_default: Option<Credentials>,
    pub ufm_auth_by_fabric: HashMap<String, Credentials>,
    pub dpu_uefi_factory_default: Option<Credentials>,
    pub dpu_uefi_site_default: Option<Credentials>,
    pub host_uefi_site_default: Option<Credentials>,
    pub nmxm_auth_by_id: HashMap<String, Credentials>,
}

impl StaticCredentialSnapshot {
    pub fn get_credentials(&self, key: &StaticCredentialKey) -> Option<Credentials> {
        match key {
            StaticCredentialKey::DpuRedfish { credential_type } => match credential_type {
                CredentialType::DpuHardwareDefault => self.dpu_redfish_factory_default.clone(),
                CredentialType::SiteDefault => self.dpu_redfish_site_default.clone(),
                CredentialType::HostHardwareDefault { .. } => None,
            },
            StaticCredentialKey::HostRedfish { credential_type } => match credential_type {
                CredentialType::HostHardwareDefault { vendor } => self
                    .host_redfish_factory_default_by_vendor
                    .get(vendor)
                    .cloned(),
                CredentialType::SiteDefault => self.host_redfish_site_default.clone(),
                CredentialType::DpuHardwareDefault => None,
            },
            StaticCredentialKey::UfmAuth { fabric } => self.ufm_auth_by_fabric.get(fabric).cloned(),
            StaticCredentialKey::DpuUefi { credential_type } => match credential_type {
                CredentialType::DpuHardwareDefault => self.dpu_uefi_factory_default.clone(),
                CredentialType::SiteDefault => self.dpu_uefi_site_default.clone(),
                CredentialType::HostHardwareDefault { .. } => None,
            },
            StaticCredentialKey::HostUefi { credential_type } => match credential_type {
                CredentialType::SiteDefault => self.host_uefi_site_default.clone(),
                _ => None,
            },
            StaticCredentialKey::NmxM { nmxm_id } => self.nmxm_auth_by_id.get(nmxm_id).cloned(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StaticCredentialKey {
    DpuRedfish { credential_type: CredentialType },
    HostRedfish { credential_type: CredentialType },
    UfmAuth { fabric: String },
    DpuUefi { credential_type: CredentialType },
    HostUefi { credential_type: CredentialType },
    NmxM { nmxm_id: String },
}

impl fmt::Display for StaticCredentialKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StaticCredentialKey::DpuRedfish { credential_type } => {
                write!(f, "DpuRedfish({credential_type:?})")
            }
            StaticCredentialKey::HostRedfish { credential_type } => {
                write!(f, "HostRedfish({credential_type:?})")
            }
            StaticCredentialKey::UfmAuth { fabric } => write!(f, "UfmAuth({fabric})"),
            StaticCredentialKey::DpuUefi { credential_type } => {
                write!(f, "DpuUefi({credential_type:?})")
            }
            StaticCredentialKey::HostUefi { credential_type } => {
                write!(f, "HostUefi({credential_type:?})")
            }
            StaticCredentialKey::NmxM { nmxm_id } => write!(f, "NmxM({nmxm_id})"),
        }
    }
}

#[async_trait]
pub trait StaticCredentialReader: Send + Sync {
    async fn get_credentials(
        &self,
        key: &StaticCredentialKey,
    ) -> Result<Option<Credentials>, SecretsError>;
}

#[async_trait]
impl StaticCredentialReader for StaticCredentialSnapshot {
    async fn get_credentials(
        &self,
        key: &StaticCredentialKey,
    ) -> Result<Option<Credentials>, SecretsError> {
        Ok(self.get_credentials(key))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct StaticCredentialsConfig {
    pub env: Option<EnvStaticCredentialsConfig>,
    pub file: Option<FileStaticCredentialsConfig>,
}

impl Default for StaticCredentialsConfig {
    fn default() -> Self {
        Self {
            env: Some(EnvStaticCredentialsConfig::default()),
            file: None,
        }
    }
}

pub struct ChainedStaticCredentialProvider {
    providers: Vec<Box<dyn StaticCredentialReader>>,
}

impl ChainedStaticCredentialProvider {
    pub fn new(providers: Vec<Box<dyn StaticCredentialReader>>) -> Self {
        Self { providers }
    }
}

#[async_trait]
impl StaticCredentialReader for ChainedStaticCredentialProvider {
    async fn get_credentials(
        &self,
        key: &StaticCredentialKey,
    ) -> Result<Option<Credentials>, SecretsError> {
        for provider in &self.providers {
            if let Some(credentials) = provider.get_credentials(key).await? {
                return Ok(Some(credentials));
            }
        }
        Ok(None)
    }
}

pub async fn create_static_credential_provider(
    config: StaticCredentialsConfig,
    vault_provider: Arc<dyn SecretPathReader>,
) -> Result<Arc<dyn StaticCredentialReader>, SecretsError> {
    let mut providers: Vec<Box<dyn StaticCredentialReader>> = Vec::new();

    if let Some(env_config) = config.env {
        providers.push(Box::new(EnvStaticCredentialsProvider::new(env_config)?));
    }

    if let Some(file_config) = config.file {
        providers.push(Box::new(
            FileStaticCredentialsProvider::new(file_config).await?,
        ));
    }

    providers.push(Box::new(VaultBackedStaticCredentialReader::new(
        vault_provider,
    )));

    Ok(Arc::new(ChainedStaticCredentialProvider::new(providers)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;
    use crate::credentials::{CredentialType, TestCredentialManager};

    #[tokio::test]
    async fn static_chain_reads_from_vault_fallback() {
        let vault_provider = TestCredentialManager::new(Credentials::UsernamePassword {
            username: "u".to_string(),
            password: "p".to_string(),
        });

        let static_provider = create_static_credential_provider(
            StaticCredentialsConfig::default(),
            Arc::new(vault_provider),
        )
        .await
        .expect("create static provider");

        let static_key = StaticCredentialKey::DpuUefi {
            credential_type: CredentialType::SiteDefault,
        };
        let value = static_provider
            .get_credentials(&static_key)
            .await
            .expect("get credentials");

        assert!(value.is_some());
    }

    #[tokio::test]
    // Mutates process environment variables. Keep serialized to avoid cross-test interference.
    #[serial]
    async fn precedence_is_env_then_file_then_vault() {
        let dir = tempdir().expect("create temp dir");
        let file_path = dir.path().join("static-creds.yaml");
        tokio::fs::write(
            &file_path,
            r#"dpu_uefi_site_default:
  username: file-user
  password: file-password
"#,
        )
        .await
        .expect("write static credential file");

        let vault_provider = TestCredentialManager::new(Credentials::UsernamePassword {
            username: "vault-user".to_string(),
            password: "vault-password".to_string(),
        });
        let static_key = StaticCredentialKey::DpuUefi {
            credential_type: CredentialType::SiteDefault,
        };

        let config = StaticCredentialsConfig {
            env: Some(EnvStaticCredentialsConfig {
                prefix: "CARBIDE_TEST_STATIC_".to_string(),
            }),
            file: Some(FileStaticCredentialsConfig {
                path: file_path,
                poll_interval: Duration::from_millis(250),
            }),
        };

        let env_user = "CARBIDE_TEST_STATIC__DPU_UEFI_SITE_DEFAULT__USERNAME";
        let env_password = "CARBIDE_TEST_STATIC__DPU_UEFI_SITE_DEFAULT__PASSWORD";
        unsafe {
            std::env::set_var(env_user, "env-user");
            std::env::set_var(env_password, "env-password");
        }

        let provider = create_static_credential_provider(config, Arc::new(vault_provider))
            .await
            .expect("create provider");

        let env_value = provider
            .get_credentials(&static_key)
            .await
            .expect("get env value");
        assert_eq!(
            env_value,
            Some(Credentials::UsernamePassword {
                username: "env-user".to_string(),
                password: "env-password".to_string(),
            })
        );

        unsafe {
            std::env::remove_var(env_user);
            std::env::remove_var(env_password);
        }

        let file_value = provider
            .get_credentials(&static_key)
            .await
            .expect("get file value");
        assert_eq!(
            file_value,
            Some(Credentials::UsernamePassword {
                username: "file-user".to_string(),
                password: "file-password".to_string(),
            })
        );
    }
}
