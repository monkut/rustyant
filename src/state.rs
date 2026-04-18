use std::sync::Arc;

use aws_sdk_s3::Client as S3Client;

use crate::Settings;
use crate::storage::{S3Storage, Storage};

#[derive(Debug, Clone)]
pub struct State {
    pub settings: Arc<Settings>,
    pub storage: Arc<dyn Storage>,
}

impl State {
    pub async fn from_env() -> crate::Result<Self> {
        let settings = Settings::from_env()?;
        let config = aws_config::load_from_env().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&config);
        if let Some(url) = &settings.aws_endpoint_url {
            builder = builder.endpoint_url(url).force_path_style(true);
        }
        let s3 = S3Client::from_conf(builder.build());
        let storage = S3Storage::new(s3, settings.bucket.clone(), settings.key_prefix.clone());
        Ok(Self { settings: Arc::new(settings), storage: Arc::new(storage) })
    }

    #[must_use]
    pub fn with_storage(settings: Settings, storage: Arc<dyn Storage>) -> Self {
        Self { settings: Arc::new(settings), storage }
    }
}
