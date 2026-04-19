use thiserror::Error;

pub type Result<T> = std::result::Result<T, RustyAntError>;

#[derive(Debug, Error)]
pub enum RustyAntError {
    #[error("config: {0}")]
    Config(String),

    #[error("resp parse: {0}")]
    RespParse(String),

    #[error("unknown command: {0}")]
    UnknownCommand(String),

    #[error("wrong number of arguments for '{command}'")]
    WrongArity { command: String },

    #[error("wrong type for key '{key}'")]
    WrongType { key: String },

    #[error("parse: {0}")]
    Parse(String),

    #[error("s3: {0}")]
    S3(String),

    #[error("too much contention on key — retries exhausted")]
    Contention,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}
