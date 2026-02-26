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
use async_trait::async_trait;
use figment::Figment;
use figment::providers::{Env, Serialized};
use serde::{Deserialize, Serialize};

use super::{StaticCredentialKey, StaticCredentialReader, StaticCredentialSnapshot};
use crate::SecretsError;
use crate::credentials::Credentials;

const DEFAULT_ENV_PREFIX: &str = "CARBIDE_STATIC_CREDENTIAL_";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct EnvStaticCredentialsConfig {
    pub prefix: String,
}

impl Default for EnvStaticCredentialsConfig {
    fn default() -> Self {
        Self {
            prefix: DEFAULT_ENV_PREFIX.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvStaticCredentialsProvider {
    snapshot: StaticCredentialSnapshot,
}

impl EnvStaticCredentialsProvider {
    pub fn new(config: EnvStaticCredentialsConfig) -> Result<Self, SecretsError> {
        let env_prefix = format!("{}__", config.prefix.trim_end_matches('_'));
        let snapshot: StaticCredentialSnapshot = Figment::new()
            .merge(Serialized::defaults(StaticCredentialSnapshot::default()))
            .merge(Env::prefixed(&env_prefix).split("__"))
            .extract()
            .map_err(|err| {
                SecretsError::GenericError(eyre::eyre!(
                    "invalid static credentials from env prefix {env_prefix}: {err}"
                ))
            })?;
        Ok(Self { snapshot })
    }
}

#[async_trait]
impl StaticCredentialReader for EnvStaticCredentialsProvider {
    async fn get_credentials(
        &self,
        key: &StaticCredentialKey,
    ) -> Result<Option<Credentials>, SecretsError> {
        Ok(self.snapshot.get_credentials(key))
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::credentials::{CredentialType, Credentials};
    use crate::static_credentials::StaticCredentialKey;

    fn env_name(parts: &[&str]) -> String {
        let mut name = "CARBIDE_STATIC_CREDENTIAL".to_string();
        for part in parts {
            name.push_str("__");
            name.push_str(part);
        }
        name
    }

    #[tokio::test]
    // Mutates process environment variables. Keep serialized to avoid cross-test interference.
    #[serial]
    async fn parses_credentials_from_env_config_style() {
        let key = StaticCredentialKey::DpuUefi {
            credential_type: CredentialType::SiteDefault,
        };
        let user_env = env_name(&["DPU_UEFI_SITE_DEFAULT", "USERNAME"]);
        let pass_env = env_name(&["DPU_UEFI_SITE_DEFAULT", "PASSWORD"]);

        unsafe {
            std::env::set_var(&user_env, "operator");
            std::env::set_var(&pass_env, "secret");
        }
        let provider = EnvStaticCredentialsProvider::new(EnvStaticCredentialsConfig::default())
            .expect("create env provider");
        let credentials = provider
            .get_credentials(&key)
            .await
            .expect("parse env credentials");
        unsafe {
            std::env::remove_var(&user_env);
            std::env::remove_var(&pass_env);
        }

        assert_eq!(
            credentials,
            Some(Credentials::UsernamePassword {
                username: "operator".to_string(),
                password: "secret".to_string(),
            })
        );
    }

    #[tokio::test]
    // Mutates process environment variables. Keep serialized to avoid cross-test interference.
    #[serial]
    async fn snapshots_env_at_startup() {
        let key = StaticCredentialKey::DpuUefi {
            credential_type: CredentialType::SiteDefault,
        };
        let user_env = env_name(&["DPU_UEFI_SITE_DEFAULT", "USERNAME"]);
        let pass_env = env_name(&["DPU_UEFI_SITE_DEFAULT", "PASSWORD"]);

        unsafe {
            std::env::set_var(&user_env, "operator");
            std::env::set_var(&pass_env, "initial");
        }

        let provider = EnvStaticCredentialsProvider::new(EnvStaticCredentialsConfig::default())
            .expect("create env provider");

        unsafe {
            std::env::set_var(&pass_env, "updated");
        }

        let credentials = provider
            .get_credentials(&key)
            .await
            .expect("read startup snapshot");

        unsafe {
            std::env::remove_var(&user_env);
            std::env::remove_var(&pass_env);
        }

        assert_eq!(
            credentials,
            Some(Credentials::UsernamePassword {
                username: "operator".to_string(),
                password: "initial".to_string(),
            })
        );
    }

    #[tokio::test]
    // Mutates process environment variables. Keep serialized to avoid cross-test interference.
    #[serial]
    async fn rejects_unknown_host_vendor_in_env_payload() {
        let user_env = env_name(&[
            "HOST_REDFISH_FACTORY_DEFAULT_BY_VENDOR",
            "NOT_A_VENDOR",
            "USERNAME",
        ]);
        let pass_env = env_name(&[
            "HOST_REDFISH_FACTORY_DEFAULT_BY_VENDOR",
            "NOT_A_VENDOR",
            "PASSWORD",
        ]);
        unsafe {
            std::env::set_var(&user_env, "operator");
            std::env::set_var(&pass_env, "secret");
        }
        let result = EnvStaticCredentialsProvider::new(EnvStaticCredentialsConfig::default());
        unsafe {
            std::env::remove_var(&user_env);
            std::env::remove_var(&pass_env);
        }
        assert!(result.is_err());
    }
}
