//! Fibonacci client demonstrating synchronous ChannelPair usage.
//!
//! This example shows:
//! - Multi-threaded send/receive with ChannelPair
//! - Bounded queue configuration
//! - Proper shutdown handling
//!
//! Run the server first: `cargo run --bin server`
//! Then run: `cargo run --bin client`

use log::{error, info};
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use threadsafe_zmq::ChannelPairBuilder;
use zmq::Context;

const NUM_REQUESTS: usize = 20;
const QUEUE_DEPTH: usize = 100;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let addr = "tcp://localhost:5555";
    let ctx = Context::new();

    // DEALER socket pairs with ROUTER on server side.
    // Unlike REQ, DEALER allows async request/response without waiting.
    let socket = ctx.socket(zmq::DEALER)?;

    // Identity is required for ROUTER to route responses back.
    // Without it, ROUTER would have no way to address us.
    let client_id = format!("client-{}", std::process::id());
    socket.set_identity(client_id.as_bytes())?;
    socket.connect(addr)?;

    let channel = ChannelPairBuilder::new(&ctx, socket)
        .with_bounded_queue(QUEUE_DEPTH)
        .build()?;

    info!("Connected to {}", addr);
    info!("Client ID: {}", client_id);

    // Shared state for coordinating sender and receiver threads
    let running = Arc::new(AtomicBool::new(true));
    let sent_count = Arc::new(AtomicUsize::new(0));
    let recv_count = Arc::new(AtomicUsize::new(0));

    // Sender thread: generates random fibonacci requests
    let channel_clone = Arc::clone(&channel);
    let running_clone = Arc::clone(&running);
    let sent_count_clone = Arc::clone(&sent_count);

    let sender_handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();

        for i in 0..NUM_REQUESTS {
            if !running_clone.load(Ordering::SeqCst) {
                break;
            }

            // Random n between 1 and 50 gives interesting but fast fibonacci values
            let n: u64 = rng.gen_range(1..=50);
            let msg = vec![n.to_string().into_bytes()];

            match channel_clone.send(msg) {
                Ok(_) => {
                    info!("[TX] Request {}: fib({})", i + 1, n);
                    sent_count_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    error!("[TX] Send error: {}", e);
                    break;
                }
            }

            // Pace requests to avoid overwhelming the server
            thread::sleep(Duration::from_millis(100));
        }

        info!("[TX] Sender finished");
    });

    // Receiver thread: collects responses
    let channel_clone = Arc::clone(&channel);
    let running_clone = Arc::clone(&running);
    let recv_count_clone = Arc::clone(&recv_count);

    let receiver_handle = thread::spawn(move || {
        loop {
            match channel_clone.recv_timeout(Duration::from_secs(2)) {
                Ok(message) => {
                    if let Some(frame) = message.last() {
                        if let Ok(result) = String::from_utf8(frame.clone()) {
                            info!("[RX] Response: {}", result);
                            recv_count_clone.fetch_add(1, Ordering::SeqCst);
                        }
                    }

                    if recv_count_clone.load(Ordering::SeqCst) >= NUM_REQUESTS {
                        break;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !running_clone.load(Ordering::SeqCst) {
                        break;
                    }
                    // Timeout is normal - continue waiting
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    info!("[RX] Channel disconnected");
                    break;
                }
            }
        }

        info!("[RX] Receiver finished");
    });

    // Wait for sender to complete
    sender_handle.join().expect("Sender thread panicked");

    // Give receiver time to collect remaining responses
    thread::sleep(Duration::from_secs(1));
    running.store(false, Ordering::SeqCst);

    receiver_handle.join().expect("Receiver thread panicked");

    // Summary
    info!("----------------------------------------");
    info!(
        "Complete: sent={}, received={}",
        sent_count.load(Ordering::SeqCst),
        recv_count.load(Ordering::SeqCst)
    );

    // Graceful shutdown ensures any buffered messages get sent
    channel.shutdown();

    Ok(())
}
