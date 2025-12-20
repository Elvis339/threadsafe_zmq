# threadsafe_zmq Examples

A high-throughput Fibonacci server using the pure async `AsyncChannelPair`:

- Bounded queue provides backpressure under load
- Semaphore-controlled parallelism prevents thread pool exhaustion
- Demonstrates ROUTER/DEALER pattern

### Client (Sync)

A multi-threaded client using the synchronous `ChannelPair`:

- Separate sender and receiver threads
- Demonstrates bounded queue usage
- Proper graceful shutdown handling

## Running

Start the server in one terminal:

```bash
RUST_LOG=info cargo run --bin server
```

In another terminal, run the client:

```bash
RUST_LOG=info cargo run --bin client
```

## Expected Output

Server:
```
Fibonacci server listening on tcp://*:5555
Configuration: queue_depth=1000, max_concurrent=100
fib(42) = 267914296
fib(15) = 610
...
```

Client:
```
Connected to tcp://localhost:5555
Client ID: client-12345
[TX] Request 1: fib(42)
[TX] Request 2: fib(15)
[RX] Response: 267914296
[RX] Response: 610
...
----------------------------------------
Complete: sent=20, received=20
```