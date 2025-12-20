//! Fibonacci server with request tracing.

use crossbeam_channel::bounded;
use log::{error, info};
use std::sync::Arc;
use std::thread;
use threadsafe_zmq::{ChannelPairBuilder, ZmqMessage};
use zmq::Context;

const NUM_WORKERS: usize = 10;
const QUEUE_DEPTH: usize = 1000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ctx = Context::new();
    let socket = ctx.socket(zmq::ROUTER)?;
    socket.bind("tcp://*:5555")?;

    let channel = ChannelPairBuilder::new(&ctx, socket)
        .with_bounded_queue(QUEUE_DEPTH)
        .build()?;

    info!("Listening on tcp://*:5555 | workers={}", NUM_WORKERS);

    let (work_tx, work_rx) = bounded::<ZmqMessage>(QUEUE_DEPTH);

    let mut workers = Vec::new();
    for id in 0..NUM_WORKERS {
        let channel = Arc::clone(&channel);
        let work_rx = work_rx.clone();

        workers.push(thread::spawn(move || {
            worker_loop(id, channel, work_rx);
        }));
    }

    loop {
        match channel.recv() {
            Ok(message) => {
                if work_tx.send(message).is_err() {
                    break;
                }
            }
            Err(e) => {
                error!("Receive error: {}", e);
                break;
            }
        }
    }

    drop(work_tx);
    for w in workers {
        let _ = w.join();
    }

    channel.shutdown();
    Ok(())
}

fn worker_loop(
    id: usize,
    channel: Arc<threadsafe_zmq::ChannelPair>,
    work_rx: crossbeam_channel::Receiver<ZmqMessage>,
) {
    while let Ok(message) = work_rx.recv() {
        if message.len() < 2 {
            continue;
        }

        let identity = message[0].clone();
        let payload = match message.last() {
            Some(p) => p,
            None => continue,
        };

        let payload_str = match String::from_utf8(payload.clone()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Parse "req_id:fib_n"
        let parts: Vec<&str> = payload_str.trim().split(':').collect();
        if parts.len() != 2 {
            continue;
        }

        let req_id: u64 = parts[0].parse().unwrap_or(0);
        let n: u64 = parts[1].parse().unwrap_or(0);

        let result = fibonacci(n);

        info!("[W{}] REQ {} fib({}) = {}", id, req_id, n, result);

        // Response: "req_id:result"
        let response_payload = format!("{}:{}", req_id, result);
        let response = vec![identity, response_payload.into_bytes()];

        if let Err(e) = channel.send(response) {
            error!("[W{}] Send error: {}", id, e);
            break;
        }
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
