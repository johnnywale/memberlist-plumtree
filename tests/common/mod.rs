//! Shared test utilities for memberlist-plumtree tests.
//!
//! This module provides utilities for test port allocation, avoiding
//! Windows socket permission errors (10013) and port conflicts.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Mutex;
use std::collections::HashSet;
use once_cell::sync::Lazy;

/// Global port allocator to prevent port conflicts across tests.
///
/// On Windows, ports can fail to bind due to:
/// - Hyper-V reserved ranges
/// - Windows Firewall
/// - TIME_WAIT state from recent connections
/// - Parallel test execution conflicts
///
/// This allocator verifies ports are truly available by binding before use.
pub struct PortAllocator {
    /// Next port to try (starts at a high range to avoid conflicts)
    next_port: AtomicU16,
    /// Ports currently allocated (prevents reuse during test run)
    allocated: Mutex<HashSet<u16>>,
}

impl PortAllocator {
    /// Create a new port allocator starting at the given port.
    pub fn new(start_port: u16) -> Self {
        Self {
            next_port: AtomicU16::new(start_port),
            allocated: Mutex::new(HashSet::new()),
        }
    }

    /// Allocate a single available port.
    ///
    /// This method:
    /// 1. Finds a port that's not in the allocated set
    /// 2. Verifies it's bindable on both TCP and UDP
    /// 3. Marks it as allocated to prevent reuse
    pub fn allocate(&self) -> u16 {
        self.allocate_n(1)[0]
    }

    /// Allocate N available ports.
    ///
    /// All ports are verified to be bindable before returning.
    pub fn allocate_n(&self, count: usize) -> Vec<u16> {
        let mut ports = Vec::with_capacity(count);
        let mut allocated = self.allocated.lock().unwrap();

        while ports.len() < count {
            let port = self.next_port.fetch_add(1, Ordering::SeqCst);

            // Skip if already allocated
            if allocated.contains(&port) {
                continue;
            }

            // Skip reserved/problematic port ranges on Windows
            if Self::is_problematic_port(port) {
                continue;
            }

            // Verify the port is actually available
            if Self::is_port_available(port) {
                allocated.insert(port);
                ports.push(port);
            }
        }

        ports
    }

    /// Release a previously allocated port.
    ///
    /// Call this in test cleanup to allow port reuse in subsequent tests.
    #[allow(dead_code)]
    pub fn release(&self, port: u16) {
        let mut allocated = self.allocated.lock().unwrap();
        allocated.remove(&port);
    }

    /// Release multiple ports.
    #[allow(dead_code)]
    pub fn release_all(&self, ports: &[u16]) {
        let mut allocated = self.allocated.lock().unwrap();
        for port in ports {
            allocated.remove(port);
        }
    }

    /// Check if a port is in a problematic range (Windows-specific).
    fn is_problematic_port(port: u16) -> bool {
        // Avoid well-known ports
        if port < 1024 {
            return true;
        }

        // Avoid ephemeral port range that Windows uses for outbound connections
        // Windows typically uses 49152-65535 for ephemeral ports
        if port >= 49152 {
            return true;
        }

        // Hyper-V often reserves ports in certain ranges
        // These can vary by system, but some common ones:
        // Check if port is in a commonly problematic range
        false
    }

    /// Verify a port is available by actually binding to it.
    fn is_port_available(port: u16) -> bool {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Check TCP availability
        let tcp_available = TcpListener::bind(addr).is_ok();

        // Check UDP availability (memberlist uses UDP)
        let udp_available = UdpSocket::bind(addr).is_ok();

        tcp_available && udp_available
    }
}

/// Global port allocator instance.
///
/// Starts at port 17000 to avoid common service ports and Windows reserved ranges.
pub static PORT_ALLOCATOR: Lazy<PortAllocator> = Lazy::new(|| {
    PortAllocator::new(17000)
});

/// Convenience function to allocate a single available port.
pub fn allocate_port() -> u16 {
    PORT_ALLOCATOR.allocate()
}

/// Convenience function to allocate multiple available ports.
pub fn allocate_ports(count: usize) -> Vec<u16> {
    PORT_ALLOCATOR.allocate_n(count)
}

/// RAII guard that releases ports when dropped.
///
/// Use this to ensure ports are released even if a test panics.
pub struct PortGuard {
    ports: Vec<u16>,
}

impl PortGuard {
    /// Create a new port guard that will release the given ports on drop.
    pub fn new(ports: Vec<u16>) -> Self {
        Self { ports }
    }

    /// Get the allocated ports.
    #[allow(dead_code)]
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }

    /// Get a single port (panics if more than one allocated).
    #[allow(dead_code)]
    pub fn port(&self) -> u16 {
        assert_eq!(self.ports.len(), 1, "Expected single port allocation");
        self.ports[0]
    }
}

impl Drop for PortGuard {
    fn drop(&mut self) {
        PORT_ALLOCATOR.release_all(&self.ports);
    }
}

/// Allocate a port with automatic cleanup via RAII guard.
#[allow(dead_code)]
pub fn allocate_port_guarded() -> PortGuard {
    PortGuard::new(vec![PORT_ALLOCATOR.allocate()])
}

/// Allocate multiple ports with automatic cleanup via RAII guard.
#[allow(dead_code)]
pub fn allocate_ports_guarded(count: usize) -> PortGuard {
    PortGuard::new(PORT_ALLOCATOR.allocate_n(count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_allocation() {
        let port1 = allocate_port();
        let port2 = allocate_port();

        assert_ne!(port1, port2, "Should allocate different ports");
        assert!(port1 >= 17000, "Should be in expected range");
        assert!(port2 >= 17000, "Should be in expected range");
    }

    #[test]
    fn test_port_guard_cleanup() {
        // Allocate ports with a guard
        let guard = allocate_ports_guarded(3);
        let ports = guard.ports().to_vec();

        // Verify we got 3 unique ports
        assert_eq!(ports.len(), 3);
        let unique: HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), 3, "All ports should be unique");

        // Verify all ports are in valid range
        for port in &ports {
            assert!(*port >= 17000, "Port should be in expected range");
        }

        // Verify ports are marked as allocated
        {
            let allocated = PORT_ALLOCATOR.allocated.lock().unwrap();
            for port in &ports {
                assert!(allocated.contains(port), "Port should be in allocated set");
            }
        }

        // Drop the guard
        drop(guard);

        // Verify ports are released
        {
            let allocated = PORT_ALLOCATOR.allocated.lock().unwrap();
            for port in &ports {
                assert!(!allocated.contains(port), "Port should be released after guard drop");
            }
        }
    }

    #[test]
    fn test_multiple_allocations() {
        let ports = allocate_ports(5);

        assert_eq!(ports.len(), 5);

        // All ports should be unique
        let unique: HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), 5);
    }
}
