use std::sync::Arc;

use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::Client as S3Client;

use crate::Settings;
use crate::dynamodb::{DynamoDbBackend, TableNames};
use crate::settings::BackendKind;
use crate::storage::{KVStorage, S3Storage, Storage, now_ms};

#[derive(Debug, Clone)]
pub struct State {
    pub settings: Arc<Settings>,
    pub storage: Arc<dyn Storage>,
    /// Wall-clock epoch ms captured when this `State` was constructed. Drives
    /// `INFO`'s `uptime_in_seconds` field so the number stays consistent across
    /// Lambda invocations served by the same container.
    pub started_at_ms: i64,
}

impl State {
    pub async fn from_env() -> crate::Result<Self> {
        let settings = Settings::from_env()?;
        let config = aws_config::load_from_env().await;
        let storage: Arc<dyn Storage> = match settings.backend {
            BackendKind::S3 => {
                let mut builder = aws_sdk_s3::config::Builder::from(&config);
                if let Some(url) = &settings.aws_endpoint_url {
                    builder = builder.endpoint_url(url).force_path_style(true);
                }
                let s3 = S3Client::from_conf(builder.build());
                let backend = S3Storage::new(s3, settings.bucket.clone(), settings.key_prefix.clone());
                Arc::new(KVStorage::new(backend))
            }
            BackendKind::Dynamodb => {
                let mut builder = aws_sdk_dynamodb::config::Builder::from(&config);
                if let Some(url) = &settings.aws_endpoint_url {
                    builder = builder.endpoint_url(url);
                }
                let client = DynamoClient::from_conf(builder.build());
                let tables = TableNames::with_prefix(&settings.dynamodb_table_prefix);
                let backend = DynamoDbBackend::new(client, tables);
                Arc::new(KVStorage::new(backend))
            }
        };
        Ok(Self { settings: Arc::new(settings), storage, started_at_ms: now_ms() })
    }

    #[must_use]
    pub fn with_storage(settings: Settings, storage: Arc<dyn Storage>) -> Self {
        Self { settings: Arc::new(settings), storage, started_at_ms: now_ms() }
    }
}
