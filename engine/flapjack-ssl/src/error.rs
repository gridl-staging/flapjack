//! Define the error type and result alias used throughout the flapjack-ssl crate.
use thiserror::Error;

pub type Result<T> = std::result::Result<T, FlapjackError>;

/// Represent errors that can occur during SSL certificate provisioning and management.
///
/// Variants cover configuration issues, raw SSL errors, ACME protocol failures,
/// I/O errors, X.509 parsing problems, and certificate generation failures.
#[derive(Error, Debug)]
pub enum FlapjackError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("SSL error: {0}")]
    Ssl(String),

    #[error("ACME error: {0}")]
    Acme(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ACME protocol error: {0}")]
    AcmeProtocol(#[from] instant_acme::Error),

    #[error("X.509 parsing error: {0}")]
    X509Parse(String),

    #[error("Certificate generation error: {0}")]
    CertGen(String),
}
