//! Error types for QUIC transport operations.
//!
//! Errors are classified for retry decisions:
//! - **Transient**: May succeed on retry (connection issues, timeouts)
//! - **Permanent**: Will not succeed on retry (TLS config, peer not found)

use std::io;
use std::net::SocketAddr;

use thiserror::Error;

/// Error type for QUIC transport operations.
#[derive(Debug, Error)]
pub enum QuicError {
    /// Peer ID could not be resolved to a socket address.
    ///
    /// This usually indicates stale resolver data. Consider triggering
    /// a resolver refresh or peer rediscovery when this error occurs.
    #[error("peer not found: {0} (consider refreshing resolver)")]
    PeerNotFound(String),

    /// TLS configuration error.
    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    /// Certificate loading or validation error.
    #[error("certificate error: {0}")]
    Certificate(String),

    /// Failed to bind to the local address.
    #[error("failed to bind: {0}")]
    Bind(#[source] io::Error),

    /// QUIC connection error.
    #[error("connection error: {0}")]
    Connection(#[source] quinn::ConnectionError),

    /// Failed to open a stream on the connection.
    #[error("stream error: {0}")]
    Stream(String),

    /// Failed to write data to a stream.
    #[error("write error: {0}")]
    Write(#[source] quinn::WriteError),

    /// Failed to read data from a stream.
    #[error("read error: {0}")]
    Read(#[source] quinn::ReadError),

    /// Connection handshake timed out.
    #[error("handshake to {addr} timed out after {timeout_ms}ms")]
    HandshakeTimeout {
        /// Target address that timed out.
        addr: SocketAddr,
        /// Timeout duration.
        timeout_ms: u64,
    },

    /// Connection was closed by peer or locally.
    #[error("connection closed (code={code}): {reason}")]
    ConnectionClosed {
        /// Error code (if available).
        code: u64,
        /// Reason for closure.
        reason: String,
    },

    /// Maximum number of connections reached.
    #[error("max connections reached ({current}/{max})")]
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
    #[error("0-RTT early data was rejected by server")]
    EarlyDataRejected,

    /// Invalid message format.
    #[error("invalid message: {0}")]
    InvalidMessage(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
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
        matches!(self, QuicError::ConnectionClosed { code: 0, .. })
    }

    /// Check if this error indicates a peer resolution failure.
    ///
    /// When this returns true, the caller should consider:
    /// - Triggering a resolver refresh
    /// - Initiating peer rediscovery
    /// - Removing the peer from local state if refresh fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// match transport.send_to(&peer_id, data).await {
    ///     Err(e) if e.should_refresh_resolver() => {
    ///         // Peer not found - try to refresh resolver
    ///         resolver.refresh(&peer_id).await;
    ///         // Optionally retry the send
    ///     }
    ///     Err(e) => return Err(e),
    ///     Ok(()) => {}
    /// }
    /// ```
    pub fn should_refresh_resolver(&self) -> bool {
        matches!(self, QuicError::PeerNotFound(_))
    }

    /// Get the peer ID string if this is a PeerNotFound error.
    pub fn peer_not_found_id(&self) -> Option<&str> {
        match self {
            QuicError::PeerNotFound(id) => Some(id),
            _ => None,
        }
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
