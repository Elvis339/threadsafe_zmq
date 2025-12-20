# Thread-Safe ZeroMQ

[![Crates.io](https://img.shields.io/crates/v/threadsafe_zmq.svg)](https://crates.io/crates/threadsafe_zmq)
[![Documentation](https://docs.rs/threadsafe_zmq/badge.svg)](https://docs.rs/threadsafe_zmq)
[![License](https://img.shields.io/crates/l/threadsafe_zmq.svg)](LICENSE)

A high-performance, thread-safe wrapper around ZeroMQ sockets with both synchronous and native async APIs.

## The Problem

ZeroMQ sockets are explicitly NOT thread-safe. From the ZMQ guide:
> "Do not use or close sockets except in the thread that created them."

This library solves that by isolating socket operations to dedicated threads and exposing thread-safe channels to user code.

## Features

- Thread-Safe
- Async Support
- Low Latency

## Installation

```toml
[dependencies]
threadsafe_zmq = "2.0"

# For async support (pure tokio, no crossbeam bridging)
threadsafe_zmq = { version = "2.0", features = ["async"] }
```

### System Dependencies

This library requires ZeroMQ to be installed on your system.

**Using Nix (recommended):**

```bash
# Install nix (requires just command runner: https://just.systems)
just install-nix

# List available commands
just && just shell

# Generate docs
just doc

# Or with direnv (automatic on cd)
direnv allow
```

**Manual installation:**

```bash
# macOS
brew install zeromq pkg-config

# Ubuntu/Debian
apt-get install libzmq3-dev pkg-config

# Fedora
dnf install zeromq-devel pkg-config

# Arch
pacman -S zeromq pkgconf
```

## Quick Start

### Synchronous API

```rust
use threadsafe_zmq::{ChannelPair, ChannelPairBuilder};
use zmq::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new();
    
    let server_sock = ctx.socket(zmq::PAIR)?;
    server_sock.bind("tcp://127.0.0.1:5555")?;
    
    let client_sock = ctx.socket(zmq::PAIR)?;
    client_sock.connect("tcp://127.0.0.1:5555")?;
    
    let server = ChannelPair::new(&ctx, server_sock)?;
    let client = ChannelPair::new(&ctx, client_sock)?;
    
    client.send(vec![b"Hello".to_vec()])?;
    
    let msg = server.recv()?;
    println!("Received: {:?}", String::from_utf8_lossy(&msg[0]));
    
    server.shutdown();
    client.shutdown();
    
    Ok(())
}
```

### Bounded Queue (Backpressure)

```rust
use threadsafe_zmq::ChannelPairBuilder;
use zmq::Context;

let ctx = Context::new();
let socket = ctx.socket(zmq::DEALER)?;
socket.connect("tcp://127.0.0.1:5555")?;

// Bounded queue prevents memory exhaustion under load
let channel = ChannelPairBuilder::new(&ctx, socket)
    .with_bounded_queue(1000)
    .build()?;

// try_send returns immediately if queue is full
match channel.try_send(vec![b"data".to_vec()]) {
    Ok(_) => println!("Queued"),
    Err(_) => println!("Queue full - apply backpressure"),
}
```

### Async

The async implementation uses native tokio channels

```toml
[dependencies]
threadsafe_zmq = { version = "2.0", features = ["async"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use threadsafe_zmq::{AsyncChannelPair, AsyncChannelPairBuilder};
use zmq::Context;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new();
    
    let socket = ctx.socket(zmq::DEALER)?;
    socket.connect("tcp://127.0.0.1:5555")?;
    
    let channel = AsyncChannelPairBuilder::new(&ctx, socket)
        .with_bounded_queue(100)
        .build()?;
    
    channel.send(vec![b"Hello".to_vec()]).await?;
    
    match channel.recv_timeout(Duration::from_secs(5)).await {
        Ok(msg) => println!("Got: {:?}", msg),
        Err(e) => eprintln!("Error: {}", e),
    }
    
    channel.shutdown().await;
    Ok(())
}
```

## Architecture

```
                    User Code
                       |
            +----------+----------+
            |                     |
         sender()              receiver()
            |                     |
            v                     ^
    +-------+-------+     +-------+-------+
    |   tx_chan     |     |    rx_chan    |
    | (crossbeam)   |     |  (crossbeam)  |
    +-------+-------+     +-------+-------+
            |                     ^
            v                     |
    +-------+-------+     +-------+-------+
    | Channel Bridge|     | Socket Thread |
    |    Thread     |     | (run_sockets) |
    +-------+-------+     +-------+-------+
            |                     ^
            v                     |
    +-------+-------+             |
    | z_tx PAIR     +-------------+
    | (inproc)      |
    +---------------+
            |
            v
    +-------+-------+
    |  ZMQ Socket   |
    |  (network)    |
    +---------------+
```

**Why this design?**

1. ZMQ sockets aren't thread-safe, so we isolate them in a dedicated thread
2. `zmq_poll` can't wait on channels, so we use internal PAIR sockets for signaling
3. The PAIR sockets convert channel events into pollable socket events

## Examples

See the [`example/`](example/) directory:

```bash
# Using just (recommended)
just example  # Runs server + client automatically

# Or manually in two terminals:
just server   # Terminal 1
just client   # Terminal 2
```

## Development

This project uses [Nix](https://nixos.org/) for reproducible builds and [just](https://just.systems/) as a command runner.

```bash
# Install nix (if needed)
just install-nix

# Enter development shell
just shell
# Or: nix develop

# Available commands
just          # Show all commands
```

## Error Handling

```rust
use threadsafe_zmq::ChannelPairError;

match channel.recv() {
    Ok(msg) => handle(msg),
    Err(ChannelPairError::Zmq(e)) => eprintln!("ZMQ: {}", e),
    Err(ChannelPairError::ChannelDisconnected(msg)) => eprintln!("Closed: {}", msg),
    Err(e) => eprintln!("Error: {}", e),
}
```

## Credits

Inspired by Go's [zmqchan](https://github.com/abligh/zmqchan).

## License

Apache-2.0. See [LICENSE](LICENSE).
