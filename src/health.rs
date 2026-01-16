//! Health check API for Plumtree protocol.
//!
//! Provides runtime health information for monitoring and alerting.
//!
//! ## Health Status
//!
//! The protocol can be in one of three states:
//!
//! - **Healthy**: Operating normally with good connectivity
//! - **Degraded**: Experiencing some issues but still functional
//! - **Unhealthy**: Significant issues requiring attention
//!
//! ## Example
//!
//! ```ignore
//! let health = plumtree.health();
//!
//! match health.status {
//!     HealthStatus::Healthy => println!("All systems operational"),
//!     HealthStatus::Degraded => println!("Warning: {}", health.message),
//!     HealthStatus::Unhealthy => println!("Critical: {}", health.message),
//! }
//! ```

use std::time::{Duration, Instant};

/// Overall health status of the Plumtree protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Operating normally with good connectivity and performance.
    Healthy,

    /// Experiencing some issues but still functional.
    ///
    /// Examples: low peer count, elevated graft failure rate.
    Degraded,

    /// Significant issues that may prevent reliable message delivery.
    ///
    /// Examples: no peers, very high graft failure rate, shutdown.
    Unhealthy,
}

impl HealthStatus {
    /// Check if the status is healthy.
    pub const fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if the status is degraded or worse.
    pub const fn is_degraded(&self) -> bool {
        !matches!(self, HealthStatus::Healthy)
    }

    /// Check if the status is unhealthy.
    pub const fn is_unhealthy(&self) -> bool {
        matches!(self, HealthStatus::Unhealthy)
    }
}

/// Detailed health report for the Plumtree protocol.
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Overall health status.
    pub status: HealthStatus,

    /// Human-readable status message.
    pub message: String,

    /// Peer connectivity health.
    pub peers: PeerHealth,

    /// Message delivery health.
    pub delivery: DeliveryHealth,

    /// Cache health.
    pub cache: CacheHealth,

    /// Whether the protocol is shutting down.
    pub is_shutdown: bool,

    /// When this health report was generated.
    pub timestamp: Instant,
}

impl HealthReport {
    /// Create a new health report.
    pub fn new(
        peers: PeerHealth,
        delivery: DeliveryHealth,
        cache: CacheHealth,
        is_shutdown: bool,
    ) -> Self {
        let (status, message) = Self::compute_status(&peers, &delivery, is_shutdown);

        Self {
            status,
            message,
            peers,
            delivery,
            cache,
            is_shutdown,
            timestamp: Instant::now(),
        }
    }

    /// Compute the overall status and message from component health.
    fn compute_status(
        peers: &PeerHealth,
        delivery: &DeliveryHealth,
        is_shutdown: bool,
    ) -> (HealthStatus, String) {
        if is_shutdown {
            return (HealthStatus::Unhealthy, "Protocol is shutting down".to_string());
        }

        // Check for unhealthy conditions
        if peers.total_peers == 0 {
            return (HealthStatus::Unhealthy, "No peers connected".to_string());
        }

        if delivery.graft_failure_rate > 0.8 {
            return (
                HealthStatus::Unhealthy,
                format!(
                    "High Graft failure rate: {:.1}%",
                    delivery.graft_failure_rate * 100.0
                ),
            );
        }

        // Check for degraded conditions
        if peers.eager_peers == 0 {
            return (
                HealthStatus::Degraded,
                "No eager peers - tree may not be formed".to_string(),
            );
        }

        if delivery.graft_failure_rate > 0.3 {
            return (
                HealthStatus::Degraded,
                format!(
                    "Elevated Graft failure rate: {:.1}%",
                    delivery.graft_failure_rate * 100.0
                ),
            );
        }

        if delivery.pending_grafts > 50 {
            return (
                HealthStatus::Degraded,
                format!("{} Grafts pending - possible network issues", delivery.pending_grafts),
            );
        }

        if peers.total_peers < 3 {
            return (
                HealthStatus::Degraded,
                format!("Low peer count: {} peers", peers.total_peers),
            );
        }

        (HealthStatus::Healthy, "Operating normally".to_string())
    }

    /// Check if the protocol is operational (healthy or degraded).
    pub fn is_operational(&self) -> bool {
        !self.status.is_unhealthy()
    }
}

/// Health information about peer connectivity.
#[derive(Debug, Clone)]
pub struct PeerHealth {
    /// Total number of connected peers.
    pub total_peers: usize,

    /// Number of eager peers (spanning tree).
    pub eager_peers: usize,

    /// Number of lazy peers (gossip fallback).
    pub lazy_peers: usize,

    /// Target eager peer count from configuration.
    pub target_eager_peers: usize,
}

impl PeerHealth {
    /// Check if peer count is sufficient.
    pub fn is_sufficient(&self) -> bool {
        self.eager_peers >= self.target_eager_peers.min(self.total_peers)
    }

    /// Get the eager peer ratio (0.0 to 1.0).
    pub fn eager_ratio(&self) -> f64 {
        if self.total_peers == 0 {
            0.0
        } else {
            self.eager_peers as f64 / self.total_peers as f64
        }
    }
}

/// Health information about message delivery.
#[derive(Debug, Clone)]
pub struct DeliveryHealth {
    /// Number of pending Graft requests.
    pub pending_grafts: usize,

    /// Graft failure rate (0.0 to 1.0).
    ///
    /// Calculated as failed_grafts / (successful_grafts + failed_grafts).
    pub graft_failure_rate: f64,

    /// Number of successful Grafts in the observation window.
    pub successful_grafts: u64,

    /// Number of failed Grafts in the observation window.
    pub failed_grafts: u64,
}

impl DeliveryHealth {
    /// Create from Graft statistics.
    pub fn new(pending_grafts: usize, successful_grafts: u64, failed_grafts: u64) -> Self {
        let total = successful_grafts + failed_grafts;
        let graft_failure_rate = if total == 0 {
            0.0
        } else {
            failed_grafts as f64 / total as f64
        };

        Self {
            pending_grafts,
            graft_failure_rate,
            successful_grafts,
            failed_grafts,
        }
    }
}

/// Health information about the message cache.
#[derive(Debug, Clone)]
pub struct CacheHealth {
    /// Current number of cached messages.
    pub cached_messages: usize,

    /// Maximum cache capacity.
    pub max_capacity: usize,

    /// Cache TTL setting.
    pub ttl: Duration,
}

impl CacheHealth {
    /// Get cache utilization (0.0 to 1.0).
    pub fn utilization(&self) -> f64 {
        if self.max_capacity == 0 {
            0.0
        } else {
            self.cached_messages as f64 / self.max_capacity as f64
        }
    }

    /// Check if cache is near capacity (>80% full).
    pub fn is_near_capacity(&self) -> bool {
        self.utilization() > 0.8
    }
}

/// Builder for creating health reports.
#[derive(Debug, Default)]
pub struct HealthReportBuilder {
    total_peers: usize,
    eager_peers: usize,
    lazy_peers: usize,
    target_eager_peers: usize,
    pending_grafts: usize,
    successful_grafts: u64,
    failed_grafts: u64,
    cached_messages: usize,
    max_cache_capacity: usize,
    cache_ttl: Duration,
    is_shutdown: bool,
}

impl HealthReportBuilder {
    /// Create a new health report builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set peer counts.
    pub fn peers(
        mut self,
        total: usize,
        eager: usize,
        lazy: usize,
        target_eager: usize,
    ) -> Self {
        self.total_peers = total;
        self.eager_peers = eager;
        self.lazy_peers = lazy;
        self.target_eager_peers = target_eager;
        self
    }

    /// Set Graft statistics.
    pub fn grafts(mut self, pending: usize, successful: u64, failed: u64) -> Self {
        self.pending_grafts = pending;
        self.successful_grafts = successful;
        self.failed_grafts = failed;
        self
    }

    /// Set cache statistics.
    pub fn cache(mut self, messages: usize, max_capacity: usize, ttl: Duration) -> Self {
        self.cached_messages = messages;
        self.max_cache_capacity = max_capacity;
        self.cache_ttl = ttl;
        self
    }

    /// Set shutdown status.
    pub fn shutdown(mut self, is_shutdown: bool) -> Self {
        self.is_shutdown = is_shutdown;
        self
    }

    /// Build the health report.
    pub fn build(self) -> HealthReport {
        let peers = PeerHealth {
            total_peers: self.total_peers,
            eager_peers: self.eager_peers,
            lazy_peers: self.lazy_peers,
            target_eager_peers: self.target_eager_peers,
        };

        let delivery = DeliveryHealth::new(
            self.pending_grafts,
            self.successful_grafts,
            self.failed_grafts,
        );

        let cache = CacheHealth {
            cached_messages: self.cached_messages,
            max_capacity: self.max_cache_capacity,
            ttl: self.cache_ttl,
        };

        HealthReport::new(peers, delivery, cache, self.is_shutdown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_healthy_status() {
        let report = HealthReportBuilder::new()
            .peers(10, 3, 7, 3)
            .grafts(5, 100, 5)
            .cache(500, 10000, Duration::from_secs(60))
            .build();

        assert_eq!(report.status, HealthStatus::Healthy);
        assert!(report.is_operational());
    }

    #[test]
    fn test_unhealthy_no_peers() {
        let report = HealthReportBuilder::new()
            .peers(0, 0, 0, 3)
            .build();

        assert_eq!(report.status, HealthStatus::Unhealthy);
        assert!(!report.is_operational());
        assert!(report.message.contains("No peers"));
    }

    #[test]
    fn test_unhealthy_high_graft_failure() {
        let report = HealthReportBuilder::new()
            .peers(10, 3, 7, 3)
            .grafts(0, 10, 90) // 90% failure rate
            .build();

        assert_eq!(report.status, HealthStatus::Unhealthy);
        assert!(report.message.contains("High Graft failure"));
    }

    #[test]
    fn test_degraded_no_eager_peers() {
        let report = HealthReportBuilder::new()
            .peers(5, 0, 5, 3)
            .build();

        assert_eq!(report.status, HealthStatus::Degraded);
        assert!(report.is_operational());
        assert!(report.message.contains("No eager peers"));
    }

    #[test]
    fn test_degraded_low_peer_count() {
        let report = HealthReportBuilder::new()
            .peers(2, 1, 1, 3)
            .build();

        assert_eq!(report.status, HealthStatus::Degraded);
        assert!(report.message.contains("Low peer count"));
    }

    #[test]
    fn test_shutdown_status() {
        let report = HealthReportBuilder::new()
            .peers(10, 3, 7, 3)
            .shutdown(true)
            .build();

        assert_eq!(report.status, HealthStatus::Unhealthy);
        assert!(report.message.contains("shutting down"));
    }

    #[test]
    fn test_cache_utilization() {
        let cache = CacheHealth {
            cached_messages: 8500,
            max_capacity: 10000,
            ttl: Duration::from_secs(60),
        };

        assert_eq!(cache.utilization(), 0.85);
        assert!(cache.is_near_capacity());
    }

    #[test]
    fn test_peer_ratio() {
        let peers = PeerHealth {
            total_peers: 10,
            eager_peers: 3,
            lazy_peers: 7,
            target_eager_peers: 3,
        };

        assert_eq!(peers.eager_ratio(), 0.3);
        assert!(peers.is_sufficient());
    }
}
