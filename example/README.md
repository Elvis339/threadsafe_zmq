# threadsafe_zmq Examples

## Sync (Recommended for throughput)

Multi-threaded server/client using `ChannelPair`:

- Server: 10 worker threads
- Client: 64 sender threads, 6400 requests
- Port: 5555

```bash
# Terminal 1
RUST_LOG=info cargo run --bin server

# Terminal 2
RUST_LOG=info cargo run --bin client
```

## Async

Tokio-based server/client using `AsyncChannelPair`:

- Server: 100 concurrent tasks
- Client: 64 sender tasks, 6400 requests
- Port: 5556

```bash
# Terminal 1
RUST_LOG=info cargo run --bin async_server

# Terminal 2
RUST_LOG=info cargo run --bin async_client
```

## Quick Start

From project root:

```bash
just example  # Runs sync server + client
```
