//! Async Fibonacci server demonstrating high-throughput message handling.
//!
//! This example shows:
//! - Pure async ZMQ with `AsyncChannelPair`
//! - Bounded queues for backpressure under load
//! - Concurrent request handling with controlled parallelism
//!
//! Run with: `cargo run --bin server`
//! Then run the client: `cargo run --bin client`

use log::{error, info, warn};
use std::sync::Arc;
use threadsafe_zmq::{AsyncChannelPairBuilder, ZmqMessage};
use tokio::sync::mpsc;
use zmq::Context;

// Limit concurrent CPU-bound work to avoid overwhelming the system.
// Fibonacci calculation is CPU-bound, so we cap parallelism to prevent
// thread starvation and maintain responsive request handling.
const MAX_CONCURRENT_REQUESTS: usize = 100;

// Queue depth provides backpressure when clients send faster than we can process.
// This prevents unbounded memory growth under load - clients will block
// when we're overloaded rather than queueing indefinitely.
const QUEUE_DEPTH: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let addr = "tcp://*:5555";
    let ctx = Context::new();

    // ROUTER socket allows multiple clients to connect.
    // Each message includes the client's identity frame for routing responses.
    let socket = ctx.socket(zmq::ROUTER)?;
    socket.bind(addr)?;

    let channel = Arc::new(
        AsyncChannelPairBuilder::new(&ctx, socket)
            .with_bounded_queue(QUEUE_DEPTH)
            .build()?
    );

    info!("Fibonacci server listening on {}", addr);
    info!(
        "Configuration: queue_depth={}, max_concurrent={}",
        QUEUE_DEPTH, MAX_CONCURRENT_REQUESTS
    );

    // Semaphore limits concurrent request processing.
    // Without this, a burst of requests could spawn unlimited tasks,
    // leading to thread pool exhaustion and degraded latency.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_REQUESTS));

    // Response channel for workers to send back results
    let (response_tx, mut response_rx) = mpsc::channel::<ZmqMessage>(QUEUE_DEPTH);

    // Spawn response sender task
    let channel_for_responses = Arc::clone(&channel);
    tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            if let Err(e) = channel_for_responses.send(response).await {
                error!("Failed to send response: {}", e);
            }
        }
    });

    loop {
        match channel.recv().await {
            Ok(message) => {
                let response_tx = response_tx.clone();
                let permit = semaphore.clone().acquire_owned().await?;

                tokio::spawn(async move {
                    handle_request(message, response_tx).await;
                    drop(permit);
                });
            }
            Err(e) => {
                error!("Fatal receive error: {}", e);
                break;
            }
        }
    }

    channel.shutdown().await;
    Ok(())
}

async fn handle_request(message: ZmqMessage, response_tx: mpsc::Sender<ZmqMessage>) {
    // ROUTER message format: [identity, empty_delimiter, ...payload]
    // The identity frame is automatically added by the ROUTER socket
    // and must be preserved for routing the response back.
    if message.len() < 2 {
        warn!("Malformed message: expected at least 2 frames, got {}", message.len());
        return;
    }

    let identity = message[0].clone();
    let payload = message.last().unwrap();

    // Parse the fibonacci number from the payload
    let number = match String::from_utf8(payload.clone()) {
        Ok(s) => match s.trim().parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                warn!("Invalid number format: {}", s);
                return;
            }
        },
        Err(_) => {
            warn!("Payload is not valid UTF-8");
            return;
        }
    };

    // Fibonacci is CPU-bound, so we offload to blocking thread pool.
    // This keeps the async executor free to handle other requests.
    let result = tokio::task::spawn_blocking(move || fibonacci(number))
        .await
        .unwrap_or(0);

    // Send response back through the channel.
    // Include identity frame so ROUTER routes to correct client.
    let response = vec![identity, result.to_string().into_bytes()];

    if response_tx.send(response).await.is_ok() {
        info!("fib({}) = {}", number, result);
    }
}

fn fibonacci(n: u64) -> u64 {
    if n <= 1 {
        return n;
    }

    let mut a: u64 = 0;
    let mut b: u64 = 1;

    for _ in 2..=n {
        let temp = a.saturating_add(b);
        a = b;
        b = temp;
    }

    b
}
