# Thread-Safe ZeroMQ

[![Crates.io](https://img.shields.io/crates/v/threadsafe_zmq.svg)](https://crates.io/crates/threadsafe_zmq)
[![Documentation](https://docs.rs/threadsafe_zmq/badge.svg)](https://docs.rs/threadsafe_zmq)
[![License](https://img.shields.io/crates/l/threadsafe_zmq.svg)](LICENSE)

Thread-safe wrapper for ZeroMQ sockets.

## The Problem

ZeroMQ sockets are not thread-safe. From the ZMQ Guide:

> "Do not use or close sockets except in the thread that created them."

If you have a multi-threaded server where workers need to send through a shared socket, you'll get SIGSEGV crashes. Common workarounds like mutexes or proxies have significant overhead.

This library wraps ZMQ sockets in a dedicated thread and exposes channel handles that are `Send + Sync`.

Thread-safe ZeroMQ wrapper for multi-threaded servers.

Trade-off: ~9x latency overhead per message, but enables parallel sending from multiple threads - which can result in higher total throughput than single-threaded raw ZMQ.

## Installation

```toml
[dependencies]
threadsafe_zmq = "2.0"

# For async support
threadsafe_zmq = { version = "2.0", features = ["async"] }
```

Requires ZeroMQ on your system:

```bash
# macOS
brew install zeromq pkg-config

# Ubuntu/Debian
apt-get install libzmq3-dev pkg-config
```

Or use Nix: `just install-nix && just shell`

## Usage

Server with worker threads:

```rust
use threadsafe_zmq::ChannelPairBuilder;
use crossbeam_channel::bounded;
use std::sync::Arc;
use std::thread;

let ctx = zmq::Context::new();
let socket = ctx.socket(zmq::ROUTER)?;
socket.bind("tcp://*:5555")?;

let channel = ChannelPairBuilder::new(&ctx, socket)
    .with_bounded_queue(1000)
    .build()?;

// Work queue for distributing messages to workers
let (work_tx, work_rx) = bounded(1000);

// Spawn workers - each can send responses through the shared channel
for _ in 0..4 {
    let ch = Arc::clone(&channel);
    let rx = work_rx.clone();
    thread::spawn(move || {
        while let Ok(msg) = rx.recv() {
            // Process and respond - safe from any thread
            ch.send(vec![b"response".to_vec()]).unwrap();
        }
    });
}

// Main loop receives and dispatches to workers
loop {
    let msg = channel.recv()?;
    work_tx.send(msg)?;
}
```

## Async

```rust
use threadsafe_zmq::AsyncChannelPair;

let channel = AsyncChannelPair::new(&ctx, socket)?;

channel.send(vec![b"hello".to_vec()]).await?;
let response = channel.recv().await?;

channel.shutdown().await;
```

## Examples

See the `example/` directory for a complete fibonacci server/client:

```bash
just example       # sync version
just example-async # async version
```

## Benchmarks

```bash
just bench
```

Compares ChannelPair against raw ZMQ with mutex and proxy patterns.

## License

Apache-2.0
