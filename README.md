# Thread-Safe ZeroMQ

[![Crates.io](https://img.shields.io/crates/v/threadsafe_zmq.svg)](https://crates.io/crates/threadsafe_zmq)
[![Documentation](https://docs.rs/threadsafe_zmq/badge.svg)](https://docs.rs/threadsafe_zmq)
[![License](https://img.shields.io/crates/l/threadsafe_zmq.svg)](LICENSE)

Thread-safe ZeroMQ wrapper for multi-threaded servers.

Trade-off: ~9x latency overhead per message, but enables parallel sending from multiple threads - which can result in higher total throughput than single-threaded raw ZMQ.

## The Problem

ZeroMQ sockets are NOT thread-safe:

> "Do not use or close sockets except in the thread that created them." - ZMQ Guide

In a multi-threaded server handling 100K+ req/s, multiple worker threads need to send responses through a single ZMQ socket. Without thread-safety, this causes SIGSEGV crashes.

```
                    ┌─── Worker Thread 1 ───┐
[100K req/s] ──────►│─── Worker Thread 2 ───│──► ZMQ Socket
                    │─── Worker Thread N ───│      ↓
                    └───────────────────────┘   SIGSEGV 💥
```

This library wraps ZMQ sockets in dedicated threads and exposes thread-safe channel handles that can be cloned and shared across any number of worker threads.

## Performance

Benchmarked on Apple M1 with 64-byte messages:

| Approach | Throughput | Notes |
|----------|------------|-------|
| Raw ZMQ (1 thread) | 3.4M msg/sec | Cannot use with multi-threaded server |
| **ChannelPair (4 threads)** | **8.5M msg/sec** | Thread-safe, 2.5x faster via parallelism |

For a 100K req/s workload, this provides **85x headroom**.

Run benchmarks yourself:
```bash
just bench
```

## Installation

```toml
[dependencies]
threadsafe_zmq = "2.0"

# For async support
threadsafe_zmq = { version = "2.0", features = ["async"] }
```

### System Dependencies

Requires ZeroMQ installed on your system.

**Using Nix (recommended):**
```bash
just install-nix && just shell
```

**Manual:**
```bash
# macOS
brew install zeromq pkg-config

# Ubuntu/Debian
apt-get install libzmq3-dev pkg-config

# Fedora
dnf install zeromq-devel pkg-config
```

## Quick Start

### Sync API

```rust
use threadsafe_zmq::ChannelPair;
use std::sync::Arc;
use std::thread;

let ctx = zmq::Context::new();

let socket = ctx.socket(zmq::DEALER)?;
socket.connect("tcp://127.0.0.1:5555")?;

let channel = ChannelPair::new(&ctx, socket)?;

// Spawn worker threads - each gets a clone of the handle
for _ in 0..4 {
    let ch = Arc::clone(&channel);
    thread::spawn(move || {
        // Safe to send from any thread
        ch.send(vec![b"response".to_vec()]).unwrap();
    });
}
```

### Bounded Queue (Backpressure)

```rust
use threadsafe_zmq::ChannelPairBuilder;

let channel = ChannelPairBuilder::new(&ctx, socket)
    .with_bounded_queue(1000)
    .build()?;

// try_send returns immediately if queue is full
match channel.try_send(vec![b"data".to_vec()]) {
    Ok(_) => println!("Queued"),
    Err(_) => println!("Queue full - apply backpressure"),
}
```

### Async API

```toml
[dependencies]
threadsafe_zmq = { version = "2.0", features = ["async"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use threadsafe_zmq::AsyncChannelPair;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::DEALER)?;
    socket.connect("tcp://127.0.0.1:5555")?;
    
    let channel = AsyncChannelPair::new(&ctx, socket)?;
    
    channel.send(vec![b"Hello".to_vec()]).await?;
    let response = channel.recv().await?;
    
    channel.shutdown().await;
    Ok(())
}
```

## Architecture

```
    User Threads (N)
          │
          ▼
    ┌───────────┐
    │ Crossbeam │ ◄── Thread-safe, clone & share
    │ Channels  │
    └─────┬─────┘
          │
          ▼
    ┌───────────┐
    │  Socket   │ ◄── Single thread owns the ZMQ socket
    │  Thread   │
    └─────┬─────┘
          │
          ▼
    ┌───────────┐
    │    ZMQ    │
    │  Socket   │
    └───────────┘
```

ZMQ sockets live in a dedicated thread. User code interacts via channels that are `Send + Sync`.

## Examples

```bash
just example  # Runs server + client
```

## Development

```bash
just install-nix  # Install Nix if needed
just shell        # Enter dev environment
just              # Show all commands
just bench        # Run benchmarks
```

## License

Apache-2.0
