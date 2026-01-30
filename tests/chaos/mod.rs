//! Chaos engineering test suite for Plumtree protocol.
//!
//! These tests verify protocol correctness under adverse conditions
//! including network partitions, clock skew, and message reordering.
//!
//! Inspired by Jepsen-style testing methodology.

mod cascading_failure;
mod clock_skew;
mod harness;
mod message_reorder;
mod split_brain;
