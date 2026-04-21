//! Shared test-harness helpers.
//!
//! Builds a [`State`] backed by the real [`S3Storage`] pointed at a local
//! floci emulator, with a unique per-call key prefix so nextest's parallel
//! runners don't share state. Used by every test binary that needs a rustyant
//! [`State`] (integration, redis-py, `ws_e2e`, and the in-crate ws tests).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};

use crate::Settings;
use crate::state::State;
use crate::storage::S3Storage;

const DEFAULT_BUCKET: &str = "rustyant-ci";

/// Monotonic per-process counter. Combined with the PID this gives every
/// [`floci_state`] call a unique prefix, even under nextest's parallel runner
/// where one test binary may run several tests concurrently.
static PREFIX_SEQ: AtomicU64 = AtomicU64::new(0);

/// Build a [`State`] backed by floci-emulated S3, scoped under
/// `{scope}/{pid}/{seq}/` so no two test states can collide.
///
/// Reads `RUSTYANT_FLOCI_URL` (required) and `RUSTYANT_FLOCI_BUCKET`
/// (defaults to `rustyant-ci`). Panics with a clear message if the URL is
/// unset — silent skips have masked real coverage gaps before.
#[must_use]
pub fn floci_state(scope: &str) -> State {
    let floci_url = std::env::var("RUSTYANT_FLOCI_URL").unwrap_or_else(|_| {
        panic!(
            "RUSTYANT_FLOCI_URL is not set — tests require a running floci. \
             Run `just floci-up && just floci-seed` (locally) or ensure the CI \
             service container is healthy."
        )
    });
    let bucket = std::env::var("RUSTYANT_FLOCI_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.to_string());

    let seq = PREFIX_SEQ.fetch_add(1, Ordering::Relaxed);
    let prefix = format!("{scope}/{}/{seq}/", std::process::id());

    let creds = Credentials::new("test", "test", None, None, "floci-test");
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(floci_url.clone())
        .force_path_style(true)
        .build();
    let client = S3Client::from_conf(config);
    let storage = S3Storage::new(client, bucket.clone(), prefix.clone());

    let settings = Settings {
        bucket,
        key_prefix: prefix,
        aws_region: Some("us-east-1".to_string()),
        aws_endpoint_url: Some(floci_url),
        emf_namespace: None,
    };
    State::with_storage(settings, Arc::new(storage))
}
