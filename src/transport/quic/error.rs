//! Error types for QUIC transport operations.
//!
//! Errors are classified for retry decisions:
//! - **Transient**: May succeed on retry (connection issues, timeouts)
//! - **Permanent**: Will not succeed on retry (TLS config, peer not found)

use std::fmt;
use std::io;
use std::net::SocketAddr;

/// Error type for QUIC transport operations.
#[derive(Debug)]
pub enum QuicError {
    /// Peer ID could not be resolved to a socket address.
    PeerNotFound(String),

    /// TLS configuration error.
    TlsConfig(String),

    /// Certificate loading or validation error.
    Certificate(String),

    /// Failed to bind to the local address.
    Bind(io::Error),

    /// QUIC connection error.
    Connection(quinn::ConnectionError),

    /// Failed to open a stream on the connection.
    Stream(String),

    /// Failed to write data to a stream.
    Write(quinn::WriteError),

    /// Failed to read data from a stream.
    Read(quinn::ReadError),

    /// Connection handshake timed out.
    HandshakeTimeout {
        /// Target address that timed out.
        addr: SocketAddr,
        /// Timeout duration.
        timeout_ms: u64,
    },

    /// Connection was closed by peer or locally.
    ConnectionClosed {
        /// Error code (if available).
        code: u64,
        /// Reason for closure.
        reason: String,
    },

    /// Maximum number of connections reached.
    MaxConnectionsReached {
        /// Current connection count.
        current: usize,
        /// Maximum allowed connections.
        max: usize,
    },

    /// 0-RTT early data was rejected by the server.
    ///
    /// This happens when the server doesn't accept early data
    /// (e.g., no session ticket, anti-replay triggered).
    EarlyDataRejected,

    /// Invalid message format.
    InvalidMessage(String),

    /// Internal error.
    Internal(String),
}

impl fmt::Display for QuicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuicError::PeerNotFound(id) => {
                write!(f, "peer not found: {}", id)
            }
            QuicError::TlsConfig(msg) => {
                write!(f, "TLS configuration error: {}", msg)
            }
            QuicError::Certificate(msg) => {
                write!(f, "certificate error: {}", msg)
            }
            QuicError::Bind(err) => {
                write!(f, "failed to bind: {}", err)
            }
            QuicError::Connection(err) => {
                write!(f, "connection error: {}", err)
            }
            QuicError::Stream(msg) => {
                write!(f, "stream error: {}", msg)
            }
            QuicError::Write(err) => {
                write!(f, "write error: {}", err)
            }
            QuicError::Read(err) => {
                write!(f, "read error: {}", err)
            }
            QuicError::HandshakeTimeout { addr, timeout_ms } => {
                write!(
                    f,
                    "handshake to {} timed out after {}ms",
                    addr, timeout_ms
                )
            }
            QuicError::ConnectionClosed { code, reason } => {
                write!(f, "connection closed (code={}): {}", code, reason)
            }
            QuicError::MaxConnectionsReached { current, max } => {
                write!(
                    f,
                    "max connections reached ({}/{})",
                    current, max
                )
            }
            QuicError::EarlyDataRejected => {
                write!(f, "0-RTT early data was rejected by server")
            }
            QuicError::InvalidMessage(msg) => {
                write!(f, "invalid message: {}", msg)
            }
            QuicError::Internal(msg) => {
                write!(f, "internal error: {}", msg)
            }
        }
    }
}

impl std::error::Error for QuicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            QuicError::Bind(err) => Some(err),
            QuicError::Connection(err) => Some(err),
            QuicError::Write(err) => Some(err),
            QuicError::Read(err) => Some(err),
            _ => None,
        }
    }
}

impl QuicError {
    /// Check if this error is transient (may succeed if retried).
    pub fn is_transient(&self) -> bool {
        match self {
            // Transient errors - network/connection issues that may resolve
            QuicError::Connection(_) => true,
            QuicError::Stream(_) => true,
            QuicError::Write(_) => true,
            QuicError::Read(_) => true,
            QuicError::HandshakeTimeout { .. } => true,
            QuicError::ConnectionClosed { .. } => true,
            QuicError::MaxConnectionsReached { .. } => true,
            QuicError::EarlyDataRejected => true,

            // Permanent errors - configuration/resolution issues
            QuicError::PeerNotFound(_) => false,
            QuicError::TlsConfig(_) => false,
            QuicError::Certificate(_) => false,
            QuicError::Bind(_) => false,
            QuicError::InvalidMessage(_) => false,
            QuicError::Internal(_) => false,
        }
    }

    /// Check if this error is permanent (will not succeed with retries).
    pub fn is_permanent(&self) -> bool {
        !self.is_transient()
    }

    /// Check if this error indicates the connection was closed cleanly.
    pub fn is_clean_close(&self) -> bool {
        matches!(
            self,
            QuicError::ConnectionClosed { code: 0, .. }
        )
    }
}

impl From<quinn::ConnectionError> for QuicError {
    fn from(err: quinn::ConnectionError) -> Self {
        QuicError::Connection(err)
    }
}

impl From<quinn::WriteError> for QuicError {
    fn from(err: quinn::WriteError) -> Self {
        QuicError::Write(err)
    }
}

impl From<quinn::ReadError> for QuicError {
    fn from(err: quinn::ReadError) -> Self {
        QuicError::Read(err)
    }
}

impl From<io::Error> for QuicError {
    fn from(err: io::Error) -> Self {
        QuicError::Bind(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = QuicError::PeerNotFound("node123".to_string());
        assert!(err.to_string().contains("node123"));

        let err = QuicError::MaxConnectionsReached {
            current: 100,
            max: 100,
        };
        assert!(err.to_string().contains("100"));
    }

    #[test]
    fn test_transient_errors() {
        assert!(QuicError::HandshakeTimeout {
            addr: "127.0.0.1:8080".parse().unwrap(),
            timeout_ms: 5000,
        }
        .is_transient());

        assert!(QuicError::MaxConnectionsReached {
            current: 100,
            max: 100,
        }
        .is_transient());

        assert!(QuicError::EarlyDataRejected.is_transient());
    }

    #[test]
    fn test_permanent_errors() {
        assert!(QuicError::PeerNotFound("test".to_string()).is_permanent());
        assert!(QuicError::TlsConfig("bad config".to_string()).is_permanent());
        assert!(QuicError::Certificate("invalid cert".to_string()).is_permanent());
    }

    #[test]
    fn test_clean_close() {
        let clean = QuicError::ConnectionClosed {
            code: 0,
            reason: "goodbye".to_string(),
        };
        assert!(clean.is_clean_close());

        let not_clean = QuicError::ConnectionClosed {
            code: 1,
            reason: "error".to_string(),
        };
        assert!(!not_clean.is_clean_close());
    }
}
