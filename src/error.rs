//! Error types for the Plumtree protocol.

use crate::message::MessageId;
use std::fmt;

/// Result type alias for Plumtree operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during Plumtree operations.
#[derive(Debug)]
pub enum Error {
    /// Message payload exceeds the configured maximum size.
    MessageTooLarge {
        /// Size of the message in bytes.
        size: usize,
        /// Maximum allowed size.
        max_size: usize,
    },

    /// Message was not found in the cache.
    MessageNotFound(MessageId),

    /// Failed to encode a message.
    Encode(String),

    /// Failed to decode a message.
    Decode(String),

    /// Network send operation failed.
    Send {
        /// Target node that we failed to send to.
        target: String,
        /// Underlying error message.
        reason: String,
    },

    /// The Plumtree instance has been shut down.
    Shutdown,

    /// No peers available for broadcast.
    NoPeers,

    /// Internal channel error.
    Channel(String),

    /// Memberlist operation failed.
    Memberlist(String),

    /// Configuration error.
    Config(String),

    /// Generic IO error.
    Io(std::io::Error),

    /// Custom error with message.
    Custom(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MessageTooLarge { size, max_size } => {
                write!(
                    f,
                    "message size ({} bytes) exceeds maximum ({} bytes)",
                    size, max_size
                )
            }
            Error::MessageNotFound(id) => {
                write!(f, "message not found in cache: {}", id)
            }
            Error::Encode(msg) => {
                write!(f, "failed to encode message: {}", msg)
            }
            Error::Decode(msg) => {
                write!(f, "failed to decode message: {}", msg)
            }
            Error::Send { target, reason } => {
                write!(f, "failed to send to {}: {}", target, reason)
            }
            Error::Shutdown => {
                write!(f, "plumtree instance has been shut down")
            }
            Error::NoPeers => {
                write!(f, "no peers available for broadcast")
            }
            Error::Channel(msg) => {
                write!(f, "channel error: {}", msg)
            }
            Error::Memberlist(msg) => {
                write!(f, "memberlist error: {}", msg)
            }
            Error::Config(msg) => {
                write!(f, "configuration error: {}", msg)
            }
            Error::Io(err) => {
                write!(f, "IO error: {}", err)
            }
            Error::Custom(msg) => {
                write!(f, "{}", msg)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(err: async_channel::SendError<T>) -> Self {
        Error::Channel(err.to_string())
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(err: async_channel::RecvError) -> Self {
        Error::Channel(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::MessageTooLarge {
            size: 100000,
            max_size: 65536,
        };
        assert!(err.to_string().contains("100000"));
        assert!(err.to_string().contains("65536"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test error");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
