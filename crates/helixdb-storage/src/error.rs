use thiserror::Error;

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("checksum mismatch in {0}")]
    ChecksumMismatch(&'static str),
    #[error("corrupt storage format: {0}")]
    Corrupt(String),
}
