//! Error types for the Plumtree protocol.
//!
//! Errors are classified as either **transient** or **permanent**:
//!
//! - **Transient errors** may succeed if retried (e.g., network timeouts, temporary unavailability)
//! - **Permanent errors** will not succeed with retries (e.g., invalid configuration, message too large)
//!
//! Use [`Error::is_transient()`] to check if an operation should be retried.

use crate::message::MessageId;
use std::fmt;

/// Result type alias for Plumtree operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Classification of error types for retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Error is transient and may succeed if retried.
    ///
    /// Examples: network timeout, temporary peer unavailability, channel full.
    Transient,

    /// Error is permanent and will not succeed with retries.
    ///
    /// Examples: message too large, invalid configuration, shutdown.
    Permanent,
}

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

    /// Outgoing message queue is full (backpressure).
    ///
    /// This error indicates the system is under load and the caller
    /// should back off and retry later.
    QueueFull {
        /// Number of messages that failed to queue.
        dropped: usize,
        /// Current queue capacity.
        capacity: usize,
    },

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
            Error::QueueFull { dropped, capacity } => {
                write!(
                    f,
                    "outgoing message queue is full ({} messages dropped, capacity {})",
                    dropped, capacity
                )
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

impl Error {
    /// Get the classification of this error.
    ///
    /// Returns [`ErrorKind::Transient`] for errors that may succeed if retried,
    /// or [`ErrorKind::Permanent`] for errors that will not succeed with retries.
    pub const fn kind(&self) -> ErrorKind {
        match self {
            // Permanent errors - these will never succeed with retries
            Error::MessageTooLarge { .. } => ErrorKind::Permanent,
            Error::Config(_) => ErrorKind::Permanent,
            Error::Shutdown => ErrorKind::Permanent,
            Error::Encode(_) => ErrorKind::Permanent,
            Error::Decode(_) => ErrorKind::Permanent,

            // Transient errors - these may succeed if retried
            Error::MessageNotFound(_) => ErrorKind::Transient,
            Error::Send { .. } => ErrorKind::Transient,
            Error::NoPeers => ErrorKind::Transient,
            Error::Channel(_) => ErrorKind::Transient,
            Error::QueueFull { .. } => ErrorKind::Transient,
            Error::Memberlist(_) => ErrorKind::Transient,
            Error::Io(_) => ErrorKind::Transient,

            // Custom errors default to transient (conservative approach)
            Error::Custom(_) => ErrorKind::Transient,
        }
    }

    /// Check if this error is transient (may succeed if retried).
    ///
    /// # Example
    ///
    /// ```ignore
    /// match plumtree.broadcast(payload).await {
    ///     Ok(_) => {}
    ///     Err(e) if e.is_transient() => {
    ///         // Retry after a delay
    ///         tokio::time::sleep(Duration::from_millis(100)).await;
    ///         plumtree.broadcast(payload).await?;
    ///     }
    ///     Err(e) => return Err(e), // Permanent error, don't retry
    /// }
    /// ```
    pub const fn is_transient(&self) -> bool {
        matches!(self.kind(), ErrorKind::Transient)
    }

    /// Check if this error is permanent (will not succeed with retries).
    pub const fn is_permanent(&self) -> bool {
        matches!(self.kind(), ErrorKind::Permanent)
    }

    /// Check if this error indicates the system is shutting down.
    pub const fn is_shutdown(&self) -> bool {
        matches!(self, Error::Shutdown)
    }

    /// Check if this is a rate limiting or resource exhaustion error.
    pub fn is_resource_exhausted(&self) -> bool {
        match self {
            Error::Channel(msg) => msg.contains("full") || msg.contains("capacity"),
            Error::QueueFull { .. } => true,
            Error::NoPeers => true,
            _ => false,
        }
    }

    /// Check if this is a queue full error (backpressure).
    pub const fn is_queue_full(&self) -> bool {
        matches!(self, Error::QueueFull { .. })
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

    #[test]
    fn test_permanent_errors() {
        // MessageTooLarge is permanent
        let err = Error::MessageTooLarge {
            size: 100,
            max_size: 50,
        };
        assert!(err.is_permanent());
        assert!(!err.is_transient());
        assert_eq!(err.kind(), ErrorKind::Permanent);

        // Config errors are permanent
        let err = Error::Config("bad config".to_string());
        assert!(err.is_permanent());

        // Shutdown is permanent
        let err = Error::Shutdown;
        assert!(err.is_permanent());
        assert!(err.is_shutdown());

        // Encode/Decode errors are permanent
        let err = Error::Encode("bad format".to_string());
        assert!(err.is_permanent());

        let err = Error::Decode("invalid data".to_string());
        assert!(err.is_permanent());
    }

    #[test]
    fn test_transient_errors() {
        // Send errors are transient (network issues)
        let err = Error::Send {
            target: "node1".to_string(),
            reason: "connection refused".to_string(),
        };
        assert!(err.is_transient());
        assert!(!err.is_permanent());
        assert_eq!(err.kind(), ErrorKind::Transient);

        // NoPeers is transient (peers may join later)
        let err = Error::NoPeers;
        assert!(err.is_transient());

        // Channel errors are transient
        let err = Error::Channel("channel closed".to_string());
        assert!(err.is_transient());

        // MessageNotFound is transient (message may be cached later)
        let err = Error::MessageNotFound(MessageId::new());
        assert!(err.is_transient());

        // IO errors are transient
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        let err: Error = io_err.into();
        assert!(err.is_transient());
    }

    #[test]
    fn test_resource_exhausted() {
        let err = Error::Channel("channel full".to_string());
        assert!(err.is_resource_exhausted());

        let err = Error::NoPeers;
        assert!(err.is_resource_exhausted());

        let err = Error::Shutdown;
        assert!(!err.is_resource_exhausted());
    }

    #[test]
    fn test_queue_full_error() {
        let err = Error::QueueFull {
            dropped: 5,
            capacity: 1024,
        };

        // QueueFull is transient
        assert!(err.is_transient());
        assert!(!err.is_permanent());
        assert_eq!(err.kind(), ErrorKind::Transient);

        // QueueFull indicates resource exhaustion
        assert!(err.is_resource_exhausted());
        assert!(err.is_queue_full());

        // Check display format
        let msg = err.to_string();
        assert!(msg.contains("5"));
        assert!(msg.contains("1024"));
        assert!(msg.contains("queue"));
    }
}
