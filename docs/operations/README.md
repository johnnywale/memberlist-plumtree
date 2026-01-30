# Operations Guide

This directory contains operational documentation for running memberlist-plumtree in production.

## Quick Reference

| Document | Description |
|----------|-------------|
| [deployment.md](deployment.md) | Deployment best practices and configuration |
| [monitoring.md](monitoring.md) | Metrics, alerting thresholds, and dashboards |
| [troubleshooting.md](troubleshooting.md) | Common issues and solutions |
| [recovery.md](recovery.md) | Disaster recovery procedures |
| [tuning.md](tuning.md) | Performance tuning guide |

## Getting Started

1. Review [deployment.md](deployment.md) before your first deployment
2. Set up monitoring using [monitoring.md](monitoring.md)
3. Bookmark [troubleshooting.md](troubleshooting.md) for quick issue resolution

## Key Metrics to Watch

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `plumtree_graft_failed_total` | >5/min | >20/min | Check network connectivity |
| `plumtree_messages_duplicate_total` rate | >30% | >50% | Review eager/lazy fanout |
| `plumtree_eager_peers` | <2 | <1 | Check cluster membership |
| `plumtree_cache_size` | >80% capacity | >95% | Increase cache or reduce TTL |

## Emergency Procedures

### Cluster-Wide Message Delivery Failure

1. Check network partitions: `plumtree_messages_partitioned_total`
2. Verify cluster membership: `plumtree_total_peers`
3. Review [troubleshooting.md#message-delivery-failures](troubleshooting.md#message-delivery-failures)

### Node Isolation

1. Check peer counts: `plumtree_eager_peers`, `plumtree_lazy_peers`
2. Verify memberlist connectivity
3. See [recovery.md#isolated-node](recovery.md#isolated-node)

### Memory Growth

1. Check cache size: `plumtree_cache_size`
2. Review message rate: `plumtree_messages_broadcast_total`
3. See [tuning.md#memory-optimization](tuning.md#memory-optimization)
