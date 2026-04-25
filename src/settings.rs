use std::env;

use crate::error::RustyAntError;

/// Which storage backend the Lambda is wired to. Selected at startup via
/// `RUSTYANT_BACKEND`; defaults to `S3` for backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    S3,
    Dynamodb,
}

impl BackendKind {
    pub fn parse(s: &str) -> Result<Self, RustyAntError> {
        match s.to_ascii_lowercase().as_str() {
            "s3" => Ok(Self::S3),
            "dynamodb" | "dynamo" => Ok(Self::Dynamodb),
            other => Err(RustyAntError::Config(format!("RUSTYANT_BACKEND must be 's3' or 'dynamodb', got '{other}'"))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    /// Storage backend. `S3` and `Dynamodb` use disjoint config — the unused
    /// fields are populated to defaults but ignored at startup.
    pub backend: BackendKind,
    /// S3 bucket name. Required when `backend == S3`; populated to empty for
    /// `Dynamodb` and ignored.
    pub bucket: String,
    /// S3 key prefix (e.g. `rustyant/`). Used only by the S3 backend.
    pub key_prefix: String,
    /// Per-kind table prefix for the `DynamoDB` backend (e.g. `rustyant-`
    /// yields `rustyant-string`, `rustyant-hash`, …). Used only by the
    /// `DynamoDB` backend.
    pub dynamodb_table_prefix: String,
    pub aws_region: Option<String>,
    pub aws_endpoint_url: Option<String>,
    /// When set, each dispatched command emits a `CloudWatch` EMF line with
    /// this namespace. Unset in local dev so we don't pollute terminal output.
    pub emf_namespace: Option<String>,
}

impl Settings {
    pub fn from_env() -> crate::Result<Self> {
        let backend = env::var("RUSTYANT_BACKEND").ok().map_or(Ok(BackendKind::S3), |s| BackendKind::parse(&s))?;

        let bucket = match backend {
            BackendKind::S3 => env::var("BUCKET")
                .map_err(|_| RustyAntError::Config("BUCKET is required when RUSTYANT_BACKEND=s3".into()))?,
            BackendKind::Dynamodb => env::var("BUCKET").unwrap_or_default(),
        };
        let key_prefix = env::var("KEY_PREFIX").unwrap_or_else(|_| "rustyant/".to_string());
        let dynamodb_table_prefix =
            env::var("RUSTYANT_DYNAMODB_TABLE_PREFIX").unwrap_or_else(|_| "rustyant-".to_string());
        let aws_region = env::var("AWS_REGION").ok();
        let aws_endpoint_url = env::var("AWS_ENDPOINT_URL").ok();
        let emf_namespace = env::var("RUSTYANT_EMF_NAMESPACE").ok().filter(|s| !s.is_empty());

        Ok(Self { backend, bucket, key_prefix, dynamodb_table_prefix, aws_region, aws_endpoint_url, emf_namespace })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_parse_accepts_known_values() {
        assert_eq!(BackendKind::parse("s3").unwrap(), BackendKind::S3);
        assert_eq!(BackendKind::parse("S3").unwrap(), BackendKind::S3);
        assert_eq!(BackendKind::parse("dynamodb").unwrap(), BackendKind::Dynamodb);
        assert_eq!(BackendKind::parse("DynamoDB").unwrap(), BackendKind::Dynamodb);
        assert_eq!(BackendKind::parse("dynamo").unwrap(), BackendKind::Dynamodb);
    }

    #[test]
    fn backend_parse_rejects_unknown() {
        assert!(BackendKind::parse("redis").is_err());
        assert!(BackendKind::parse("").is_err());
    }
}
