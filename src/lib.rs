pub mod commands;
pub mod dynamodb;
pub mod error;
pub mod geo;
pub mod handler;
pub mod hll;
pub mod metrics;
pub mod rdb;
pub mod resp;
pub mod settings;
pub mod state;
pub mod storage;
pub mod stream;
pub mod test_support;
pub mod ws;

pub use error::{Result, RustyAntError};
pub use settings::Settings;
pub use state::State;

pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,rustyant=debug"));
    fmt().with_env_filter(filter).json().init();
}
