use std::env;

use crate::error::RustyAntError;

#[derive(Debug, Clone)]
pub struct Settings {
    pub bucket: String,
    pub key_prefix: String,
    pub aws_region: Option<String>,
    pub aws_endpoint_url: Option<String>,
    /// When set, each dispatched command emits a `CloudWatch` EMF line with
    /// this namespace. Unset in local dev so we don't pollute terminal output.
    pub emf_namespace: Option<String>,
}

impl Settings {
    pub fn from_env() -> crate::Result<Self> {
        let bucket = env::var("BUCKET").map_err(|_| RustyAntError::Config("BUCKET is required".into()))?;
        let key_prefix = env::var("KEY_PREFIX").unwrap_or_else(|_| "rustyant/".to_string());
        let aws_region = env::var("AWS_REGION").ok();
        let aws_endpoint_url = env::var("AWS_ENDPOINT_URL").ok();
        let emf_namespace = env::var("RUSTYANT_EMF_NAMESPACE").ok().filter(|s| !s.is_empty());

        Ok(Self { bucket, key_prefix, aws_region, aws_endpoint_url, emf_namespace })
    }
}
