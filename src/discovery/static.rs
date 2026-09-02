//! Static seed-based peer discovery.
//!
//! Discovers peers from a configured list of seed addresses.
//! Periodically probes seeds to detect availability.

use super::traits::{ClusterDiscovery, DiscoveryEvent, SimpleDiscoveryHandle};
#[cfg(feature = "tokio")]
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "tokio")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for static seed discovery.
#[derive(Debug, Clone)]
pub struct StaticDiscoveryConfig<I> {
    /// Known seed addresses with their node IDs.
    pub seeds: Vec<(I, SocketAddr)>,
    /// Interval for probing seeds.
    ///
    /// Default: 30s
    pub probe_interval: Duration,
    /// Timeout applied to each reachability probe.
    ///
    /// Only meaningful when a probe callback is installed via
    /// [`StaticDiscovery::set_reachability_probe`]. With no probe installed,
    /// seeds are assumed reachable and this value is unused.
    ///
    /// Default: 5s
    pub probe_timeout: Duration,
    /// Initial delay before first probe.
    ///
    /// Default: 0 (immediate)
    pub initial_delay: Duration,
    /// Emit all seeds immediately on start without probing.
    ///
    /// When true, all configured seeds are emitted as `PeerDiscovered`
    /// immediately on start. Probing still runs to detect lost peers.
    ///
    /// Default: true
    pub emit_on_start: bool,
}

impl<I> Default for StaticDiscoveryConfig<I> {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            probe_interval: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(5),
            initial_delay: Duration::ZERO,
            emit_on_start: true,
        }
    }
}

impl<I> StaticDiscoveryConfig<I> {
    /// Create a new static discovery configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a seed peer (builder pattern).
    pub fn with_seed(mut self, id: I, addr: SocketAddr) -> Self {
        self.seeds.push((id, addr));
        self
    }

    /// Add multiple seed peers (builder pattern).
    pub fn with_seeds(mut self, seeds: impl IntoIterator<Item = (I, SocketAddr)>) -> Self {
        self.seeds.extend(seeds);
        self
    }

    /// Set probe interval (builder pattern).
    pub const fn with_probe_interval(mut self, interval: Duration) -> Self {
        self.probe_interval = interval;
        self
    }

    /// Set probe timeout (builder pattern).
    pub const fn with_probe_timeout(mut self, timeout: Duration) -> Self {
        self.probe_timeout = timeout;
        self
    }

    /// Set initial delay before first probe (builder pattern).
    pub const fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set whether to emit seeds immediately on start (builder pattern).
    pub const fn with_emit_on_start(mut self, emit: bool) -> Self {
        self.emit_on_start = emit;
        self
    }
}

/// Parameters for the background discovery loop.
#[cfg(feature = "tokio")]
struct DiscoveryLoopParams<I> {
    seeds: Vec<(I, SocketAddr)>,
    probe_interval: Duration,
    probe_timeout: Duration,
    initial_delay: Duration,
    emit_on_start: bool,
    probe: Option<ReachabilityProbe>,
}

/// Callback deciding whether a seed address is currently reachable.
///
/// Returns `true` if the seed responded within the configured
/// [`StaticDiscoveryConfig::probe_timeout`]. See
/// [`StaticDiscovery::set_reachability_probe`].
pub type ReachabilityProbe = Arc<dyn Fn(SocketAddr, Duration) -> bool + Send + Sync>;

/// Static seed-based peer discovery.
///
/// Discovers peers from a configured list of seed addresses.
/// Optionally probes seeds periodically to detect availability changes.
///
/// # Example
///
/// ```
/// use memberlist_plumtree::discovery::{StaticDiscovery, StaticDiscoveryConfig};
/// use std::time::Duration;
///
/// let config = StaticDiscoveryConfig::new()
///     .with_seed(1u64, "192.168.1.10:9000".parse().unwrap())
///     .with_seed(2u64, "192.168.1.11:9000".parse().unwrap())
///     .with_probe_interval(Duration::from_secs(30));
///
/// let discovery = StaticDiscovery::new(config);
/// ```
///
/// By default every configured seed is treated as permanently reachable, so
/// only [`DiscoveryEvent::PeerDiscovered`] is ever emitted. Install a probe
/// with [`StaticDiscovery::set_reachability_probe`] to detect seeds going away
/// and get [`DiscoveryEvent::PeerLost`] events.
#[derive(Clone)]
pub struct StaticDiscovery<I> {
    config: StaticDiscoveryConfig<I>,
    local_addr: Option<SocketAddr>,
    probe: Option<ReachabilityProbe>,
}

impl<I: Debug> Debug for StaticDiscovery<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticDiscovery")
            .field("config", &self.config)
            .field("local_addr", &self.local_addr)
            .field("probe", &self.probe.as_ref().map(|_| "<installed>"))
            .finish()
    }
}

impl<I> StaticDiscovery<I> {
    /// Create a new static discovery with the given configuration.
    pub fn new(config: StaticDiscoveryConfig<I>) -> Self {
        Self {
            config,
            local_addr: None,
            probe: None,
        }
    }

    /// Install a reachability probe, enabling `PeerLost` events.
    ///
    /// Without a probe, static discovery treats every configured seed as
    /// permanently reachable: it emits [`DiscoveryEvent::PeerDiscovered`] once
    /// and never emits [`DiscoveryEvent::PeerLost`], and
    /// [`StaticDiscoveryConfig::probe_timeout`] goes unused. This crate does
    /// not depend on `tokio`'s `net` feature, so it cannot open sockets
    /// itself — supply the connectivity check from your application.
    ///
    /// The callback receives the seed's address and the configured
    /// `probe_timeout`, and must return within roughly that duration. It is
    /// called from a background task once per seed per `probe_interval`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::net::TcpStream;
    /// use std::sync::Arc;
    ///
    /// discovery.set_reachability_probe(Arc::new(|addr, timeout| {
    ///     TcpStream::connect_timeout(&addr, timeout).is_ok()
    /// }));
    /// ```
    pub fn set_reachability_probe(&mut self, probe: ReachabilityProbe) {
        self.probe = Some(probe);
    }

    /// Builder form of [`StaticDiscovery::set_reachability_probe`].
    pub fn with_reachability_probe(mut self, probe: ReachabilityProbe) -> Self {
        self.probe = Some(probe);
        self
    }

    /// Create a new static discovery with the local address.
    pub fn with_local_addr(mut self, addr: SocketAddr) -> Self {
        self.local_addr = Some(addr);
        self
    }

    /// Whether a reachability probe is installed.
    pub fn has_reachability_probe(&self) -> bool {
        self.probe.is_some()
    }

    /// Create from a list of seeds with default settings.
    pub fn from_seeds(seeds: impl IntoIterator<Item = (I, SocketAddr)>) -> Self {
        Self::new(StaticDiscoveryConfig::default().with_seeds(seeds))
    }
}

impl<I> ClusterDiscovery<I> for StaticDiscovery<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Handle = SimpleDiscoveryHandle;

    async fn start(&self) -> (async_channel::Receiver<DiscoveryEvent<I>>, Self::Handle) {
        let (tx, rx) = async_channel::bounded(self.config.seeds.len().max(16));
        let running = Arc::new(AtomicBool::new(true));
        let handle = SimpleDiscoveryHandle::new(running.clone());

        // Clone config for the background task
        let seeds = self.config.seeds.clone();
        let emit_on_start = self.config.emit_on_start;

        // Spawn background probe task
        #[cfg(feature = "tokio")]
        {
            let probe_interval = self.config.probe_interval;
            let probe_timeout = self.config.probe_timeout;
            let initial_delay = self.config.initial_delay;
            let probe = self.probe.clone();
            tokio::spawn(async move {
                Self::run_discovery_loop(
                    tx,
                    running,
                    DiscoveryLoopParams {
                        seeds,
                        probe_interval,
                        probe_timeout,
                        initial_delay,
                        emit_on_start,
                        probe,
                    },
                )
                .await;
            });
        }

        #[cfg(not(feature = "tokio"))]
        {
            // For non-tokio builds, emit seeds synchronously
            if emit_on_start {
                for (id, addr) in &seeds {
                    let _ = tx.try_send(DiscoveryEvent::PeerDiscovered {
                        id: id.clone(),
                        addr: *addr,
                    });
                }
            }
        }

        (rx, handle)
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    fn initial_peers(&self) -> Vec<(I, SocketAddr)> {
        self.config.seeds.clone()
    }
}

impl<I> StaticDiscovery<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    #[cfg(feature = "tokio")]
    async fn run_discovery_loop(
        tx: async_channel::Sender<DiscoveryEvent<I>>,
        running: Arc<AtomicBool>,
        params: DiscoveryLoopParams<I>,
    ) {
        let DiscoveryLoopParams {
            seeds,
            probe_interval,
            probe_timeout,
            initial_delay,
            emit_on_start,
            probe,
        } = params;

        // Initial delay
        if !initial_delay.is_zero() {
            tokio::time::sleep(initial_delay).await;
        }

        // Track known peers
        let mut known: HashMap<I, bool> = HashMap::new();

        // Emit all seeds on start if configured
        if emit_on_start {
            for (id, addr) in &seeds {
                known.insert(id.clone(), true);
                if tx
                    .send(DiscoveryEvent::PeerDiscovered {
                        id: id.clone(),
                        addr: *addr,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }

        // Probe loop
        let mut interval = tokio::time::interval(probe_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Skip first tick if emit_on_start is false (we want to wait for probe_interval)
        if !emit_on_start {
            interval.tick().await; // Consume immediate first tick
        }

        loop {
            interval.tick().await;

            if !running.load(Ordering::Acquire) {
                break;
            }

            for (id, addr) in &seeds {
                let was_known = known.get(id).copied().unwrap_or(false);

                // Probe reachability if the application supplied a check.
                // Without one, a static seed is assumed permanently reachable
                // (so `PeerLost` never fires) — see
                // `StaticDiscovery::set_reachability_probe`.
                let is_reachable = match probe.as_ref() {
                    Some(probe) => {
                        let probe = probe.clone();
                        let addr = *addr;
                        // The probe is a blocking callback; keep it off the
                        // runtime worker.
                        match tokio::task::spawn_blocking(move || probe(addr, probe_timeout)).await
                        {
                            Ok(reachable) => reachable,
                            Err(e) => {
                                tracing::warn!(?id, error = %e, "reachability probe panicked");
                                false
                            }
                        }
                    }
                    None => true,
                };

                if is_reachable && !was_known {
                    known.insert(id.clone(), true);
                    if tx
                        .send(DiscoveryEvent::PeerDiscovered {
                            id: id.clone(),
                            addr: *addr,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                } else if !is_reachable && was_known {
                    known.insert(id.clone(), false);
                    if tx
                        .send(DiscoveryEvent::PeerLost { id: id.clone() })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::DiscoveryHandle;

    #[test]
    fn test_config_builder() {
        let config = StaticDiscoveryConfig::<u64>::new()
            .with_seed(1, "127.0.0.1:9001".parse().unwrap())
            .with_seed(2, "127.0.0.1:9002".parse().unwrap())
            .with_probe_interval(Duration::from_secs(60))
            .with_probe_timeout(Duration::from_secs(10))
            .with_emit_on_start(false);

        assert_eq!(config.seeds.len(), 2);
        assert_eq!(config.probe_interval, Duration::from_secs(60));
        assert_eq!(config.probe_timeout, Duration::from_secs(10));
        assert!(!config.emit_on_start);
    }

    #[test]
    fn test_config_with_seeds() {
        let seeds = vec![
            (1u64, "127.0.0.1:9001".parse().unwrap()),
            (2u64, "127.0.0.1:9002".parse().unwrap()),
        ];

        let config = StaticDiscoveryConfig::new().with_seeds(seeds);
        assert_eq!(config.seeds.len(), 2);
    }

    #[test]
    fn test_from_seeds() {
        let seeds = vec![
            (1u64, "127.0.0.1:9001".parse().unwrap()),
            (2u64, "127.0.0.1:9002".parse().unwrap()),
        ];

        let discovery = StaticDiscovery::from_seeds(seeds);
        assert_eq!(discovery.config.seeds.len(), 2);
    }

    #[tokio::test]
    #[cfg(feature = "tokio")]
    async fn test_static_discovery_emits_seeds() {
        let discovery = StaticDiscovery::from_seeds(vec![
            (1u64, "127.0.0.1:9001".parse().unwrap()),
            (2u64, "127.0.0.1:9002".parse().unwrap()),
        ]);

        let (rx, handle) = discovery.start().await;

        // Should receive both seeds
        let event1 = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        let event2 = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        // Verify both peers were discovered
        let mut ids: Vec<u64> = vec![];
        if let DiscoveryEvent::PeerDiscovered { id, .. } = event1 {
            ids.push(id);
        }
        if let DiscoveryEvent::PeerDiscovered { id, .. } = event2 {
            ids.push(id);
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2]);

        handle.stop();
    }

    #[tokio::test]
    #[cfg(feature = "tokio")]
    async fn test_static_discovery_no_emit_on_start() {
        let config = StaticDiscoveryConfig::new()
            .with_seed(1u64, "127.0.0.1:9001".parse().unwrap())
            .with_emit_on_start(false)
            .with_probe_interval(Duration::from_secs(1)); // Long interval

        let discovery = StaticDiscovery::new(config);
        let (rx, handle) = discovery.start().await;

        // Should not receive anything immediately
        let result = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(result.is_err(), "should timeout with no immediate events");

        handle.stop();
    }

    #[test]
    fn test_local_addr() {
        let discovery = StaticDiscovery::<u64>::new(StaticDiscoveryConfig::new())
            .with_local_addr("127.0.0.1:9000".parse().unwrap());

        assert_eq!(
            discovery.local_addr(),
            Some("127.0.0.1:9000".parse().unwrap())
        );
    }
}
