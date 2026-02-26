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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use notify::{PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{StaticCredentialKey, StaticCredentialReader, StaticCredentialSnapshot};
use crate::SecretsError;
use crate::credentials::Credentials;

const DEFAULT_FILE_POLL_INTERVAL: Duration = Duration::from_secs(60);

fn default_file_poll_interval() -> Duration {
    DEFAULT_FILE_POLL_INTERVAL
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileStaticCredentialsConfig {
    pub path: PathBuf,
    #[serde(default = "default_file_poll_interval")]
    pub poll_interval: Duration,
}

impl FileStaticCredentialsConfig {
    pub fn with_default_poll_interval(path: PathBuf) -> Self {
        Self {
            path,
            poll_interval: DEFAULT_FILE_POLL_INTERVAL,
        }
    }
}

pub struct FileStaticCredentialsProvider {
    credentials: Arc<ArcSwap<StaticCredentialSnapshot>>,
    _primary_watcher: RecommendedWatcher,
    _secondary_watcher: PollWatcher,
}

impl FileStaticCredentialsProvider {
    pub async fn new(config: FileStaticCredentialsConfig) -> Result<Self, SecretsError> {
        let path = config.path;
        let poll_interval = config.poll_interval;
        let credentials = Arc::new(ArcSwap::from_pointee(Self::load_file(&path).await?));
        let (tx, mut rx) = mpsc::channel(32);

        let tx_clone = tx.clone();
        let mut primary = RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| match res {
                Ok(ref event) if event.kind.is_create() || event.kind.is_modify() => {
                    if let Err(err) = tx_clone.blocking_send(res) {
                        tracing::warn!("failed to send static credential watch event: {err}");
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!("primary static credential watcher error: {err}");
                }
            },
            notify::Config::default(),
        )
        .map_err(|err| SecretsError::GenericError(err.into()))?;

        primary
            .watch(&path, RecursiveMode::NonRecursive)
            .map_err(|err| SecretsError::GenericError(err.into()))?;

        let mut secondary = PollWatcher::new(
            move |res| {
                if let Err(err) = tx.blocking_send(res) {
                    tracing::warn!("failed to send static credential poll event: {err}");
                }
            },
            notify::Config::default()
                .with_poll_interval(poll_interval)
                .with_compare_contents(true),
        )
        .map_err(|err| SecretsError::GenericError(err.into()))?;

        secondary
            .watch(&path, RecursiveMode::NonRecursive)
            .map_err(|err| SecretsError::GenericError(err.into()))?;

        let watched_path = path.clone();
        let credentials_clone = credentials.clone();
        tokio::spawn(async move {
            while let Some(event_result) = rx.recv().await {
                match event_result {
                    Ok(event) => {
                        if !event
                            .paths
                            .iter()
                            .any(|event_path| event_path.file_name() == watched_path.file_name())
                        {
                            continue;
                        }

                        match Self::load_file(&watched_path).await {
                            Ok(updated) => {
                                credentials_clone.store(Arc::new(updated));
                            }
                            Err(err) => {
                                tracing::warn!("failed to reload static credentials file: {err}");
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!("static credential watcher event error: {err}");
                    }
                }
            }
        });

        Ok(Self {
            credentials,
            _primary_watcher: primary,
            _secondary_watcher: secondary,
        })
    }

    async fn load_file(path: &Path) -> Result<StaticCredentialSnapshot, SecretsError> {
        let content = tokio::fs::read(path)
            .await
            .map_err(|err| SecretsError::GenericError(err.into()))?;

        if let Ok(parsed) = serde_json::from_slice::<StaticCredentialSnapshot>(&content) {
            return Ok(parsed);
        }

        let parsed =
            serde_yaml::from_slice::<StaticCredentialSnapshot>(&content).map_err(|err| {
                SecretsError::GenericError(eyre::eyre!(
                    "failed to parse static credential file as JSON or YAML: {err}"
                ))
            })?;
        Ok(parsed)
    }
}

#[async_trait]
impl StaticCredentialReader for FileStaticCredentialsProvider {
    async fn get_credentials(
        &self,
        key: &StaticCredentialKey,
    ) -> Result<Option<Credentials>, SecretsError> {
        let snapshot = self.credentials.load();
        Ok(snapshot.get_credentials(key))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::credentials::{CredentialType, Credentials};
    use crate::static_credentials::StaticCredentialKey;

    #[tokio::test]
    async fn loads_json_file_and_reloads_on_change() {
        let dir = tempdir().expect("create temp dir");
        let file_path = dir.path().join("credentials.json");
        tokio::fs::write(
            &file_path,
            r#"{
  "dpu_uefi_site_default": {
    "username": "root",
    "password": "json1"
  }
}"#,
        )
        .await
        .expect("write initial json file");

        let provider = FileStaticCredentialsProvider::new(FileStaticCredentialsConfig {
            path: file_path.clone(),
            poll_interval: Duration::from_secs(1),
        })
        .await
        .expect("create file provider");

        let key = StaticCredentialKey::DpuUefi {
            credential_type: CredentialType::SiteDefault,
        };

        let first = provider
            .get_credentials(&key)
            .await
            .expect("load first value");
        assert_eq!(
            first,
            Some(Credentials::UsernamePassword {
                username: "root".to_string(),
                password: "json1".to_string(),
            })
        );

        tokio::fs::write(
            &file_path,
            r#"{
  "dpu_uefi_site_default": {
    "username": "root",
    "password": "json2"
  }
}"#,
        )
        .await
        .expect("update json file");
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let second = provider
            .get_credentials(&key)
            .await
            .expect("load reloaded value");
        assert_eq!(
            second,
            Some(Credentials::UsernamePassword {
                username: "root".to_string(),
                password: "json2".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn loads_yaml_file() {
        let dir = tempdir().expect("create temp dir");
        let file_path = dir.path().join("credentials.yaml");
        tokio::fs::write(
            &file_path,
            r#"dpu_uefi_site_default:
  username: root
  password: yaml1
"#,
        )
        .await
        .expect("write yaml file");

        let provider = FileStaticCredentialsProvider::new(FileStaticCredentialsConfig {
            path: file_path.clone(),
            poll_interval: Duration::from_secs(1),
        })
        .await
        .expect("create yaml file provider");

        let key = StaticCredentialKey::DpuUefi {
            credential_type: CredentialType::SiteDefault,
        };

        let value = provider
            .get_credentials(&key)
            .await
            .expect("load yaml value");
        assert_eq!(
            value,
            Some(Credentials::UsernamePassword {
                username: "root".to_string(),
                password: "yaml1".to_string(),
            })
        );
    }
}
