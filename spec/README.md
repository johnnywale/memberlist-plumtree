# TLA+ Specification for Plumtree Protocol

This directory contains the formal TLA+ specification of the Plumtree
(Epidemic Broadcast Trees) protocol used in memberlist-plumtree.

## Overview

The specification models the core protocol invariants:

1. **Message Delivery**: All broadcast messages are eventually delivered to all nodes
2. **No Duplicates**: Messages are delivered at most once to each node
3. **Tree Connectivity**: The spanning tree remains connected
4. **Graft Progress**: Graft requests eventually resolve (succeed or fail)

## Files

- `plumtree.tla` - Main protocol specification
- `plumtree_types.tla` - Type definitions and helpers
- `plumtree.cfg` - Model checker configuration

## Running the Model Checker

### Prerequisites

1. Install the TLA+ Toolbox or TLC command-line tools
2. Ensure Java is installed (TLC requires Java 11+)

### Using TLA+ Toolbox

1. Open TLA+ Toolbox
2. File → Open Spec → Add New Spec
3. Select `plumtree.tla`
4. Create a new model (Model → New Model)
5. Load configuration from `plumtree.cfg`
6. Run the model checker

### Using TLC Command Line

```bash
# Navigate to the spec directory
cd spec

# Run with default configuration
tlc plumtree.tla -config plumtree.cfg

# Run with specific parameters
tlc plumtree.tla -config plumtree.cfg -workers 4 -deadlock
```

### Model Parameters

The default configuration in `plumtree.cfg` uses small values for tractable model checking:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `Nodes` | `{n1, n2, n3}` | Set of nodes |
| `Messages` | `{m1, m2}` | Set of possible messages |
| `MaxRound` | `3` | Maximum gossip round number |

For larger models, increase these values but expect longer run times.

## Invariants Checked

### Safety Properties

1. **TypeInvariant**: All variables maintain their declared types
2. **NoDuplicateDelivery**: Each message delivered at most once per node
3. **NoSelfInEager**: Node never has itself in its eager set

### Liveness Properties (under fairness)

1. **EventualDelivery**: If any node receives a message, all nodes eventually receive it
2. **GraftTermination**: All graft requests eventually complete

## Limitations

This specification is an abstract model that:

- Uses a finite set of nodes and messages
- Abstracts away network latency (messages delivered atomically)
- Does not model message content (only message IDs)
- Assumes reliable delivery after chaos conditions clear

For full protocol validation, combine with:
- Unit tests (cargo test)
- Integration tests
- Chaos engineering tests (tests/chaos/)
- Fuzz testing (fuzz/)

## References

- [Plumtree Paper](https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf) - Original Plumtree protocol paper
- [TLA+ Hyperbook](https://lamport.azurewebsites.net/tla/hyperbook.html) - Learn TLA+
- [TLC Model Checker](https://lamport.azurewebsites.net/tla/tlc.html) - TLC documentation
