//! Async Fibonacci server

use log::{error, info};
use std::sync::Arc;
use threadsafe_zmq::{AsyncChannelPairBuilder, ZmqMessage};
use tokio::sync::mpsc;
use zmq::Context;

const MAX_CONCURRENT: usize = 100;
const QUEUE_DEPTH: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ctx = Context::new();
    let socket = ctx.socket(zmq::ROUTER)?;
    socket.bind("tcp://*:5556")?;

    let channel = Arc::new(
        AsyncChannelPairBuilder::new(&ctx, socket)
            .with_bounded_queue(QUEUE_DEPTH)
            .build()?,
    );

    info!("Listening on tcp://*:5556 | max_concurrent={}", MAX_CONCURRENT);

    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
    let (response_tx, mut response_rx) = mpsc::channel::<ZmqMessage>(QUEUE_DEPTH);

    let channel_for_responses = Arc::clone(&channel);
    tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            if let Err(e) = channel_for_responses.send(response).await {
                error!("Send error: {}", e);
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
                error!("Receive error: {}", e);
                break;
            }
        }
    }

    channel.shutdown().await;
    Ok(())
}

async fn handle_request(message: ZmqMessage, response_tx: mpsc::Sender<ZmqMessage>) {
    if message.len() < 2 {
        return;
    }

    let identity = message[0].clone();
    let payload = match message.last() {
        Some(p) => p,
        None => return,
    };

    let payload_str = match String::from_utf8(payload.clone()) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Parse "req_id:fib_n"
    let parts: Vec<&str> = payload_str.trim().split(':').collect();
    if parts.len() != 2 {
        return;
    }

    let req_id: u64 = parts[0].parse().unwrap_or(0);
    let n: u64 = parts[1].parse().unwrap_or(0);

    let result = tokio::task::spawn_blocking(move || fibonacci(n))
        .await
        .unwrap_or(0);

    info!("REQ {} fib({}) = {}", req_id, n, result);

    let response_payload = format!("{}:{}", req_id, result);
    let response = vec![identity, response_payload.into_bytes()];

    let _ = response_tx.send(response).await;
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
