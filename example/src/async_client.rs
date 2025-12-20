use log::info;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use threadsafe_zmq::AsyncChannelPairBuilder;
use tokio::sync::Mutex;
use zmq::Context;

const NUM_SENDERS: usize = 64;
const NUM_REQUESTS: usize = NUM_SENDERS * 100;
const QUEUE_DEPTH: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ctx = Context::new();
    let socket = ctx.socket(zmq::DEALER)?;
    socket.set_identity(b"async-client-1")?;
    socket.connect("tcp://localhost:5556")?;

    let channel = Arc::new(
        AsyncChannelPairBuilder::new(&ctx, socket)
            .with_bounded_queue(QUEUE_DEPTH)
            .build()?,
    );

    info!("Connected | senders={} requests={}", NUM_SENDERS, NUM_REQUESTS);

    let start_time = Instant::now();

    // Track pending requests: request_id -> (fib_n, send_time)
    let pending: Arc<Mutex<HashMap<u64, (u64, Instant)>>> = Arc::new(Mutex::new(HashMap::new()));

    let requests_per_sender = NUM_REQUESTS / NUM_SENDERS;

    // Sender tasks
    let mut senders = Vec::new();
    for sender_id in 0..NUM_SENDERS {
        let channel = Arc::clone(&channel);
        let pending = Arc::clone(&pending);
        let base_id = (sender_id * requests_per_sender) as u64;

        senders.push(tokio::spawn(async move {
            use rand::SeedableRng;
            let mut rng = rand::rngs::StdRng::seed_from_u64(base_id + 1);

            for i in 0..requests_per_sender {
                let req_id = base_id + i as u64;
                let n: u64 = rng.gen_range(10..=40);

                {
                    let mut p = pending.lock().await;
                    p.insert(req_id, (n, Instant::now()));
                }

                let payload = format!("{}:{}", req_id, n);
                let msg = vec![payload.into_bytes()];

                channel.send(msg).await.unwrap();
                info!("[REQ {}] --> fib({})", req_id, n);

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    // Receiver task
    let channel_clone = Arc::clone(&channel);
    let pending_clone = Arc::clone(&pending);
    let receiver = tokio::spawn(async move {
        let mut results = Vec::new();
        let mut count = 0;

        while count < NUM_REQUESTS {
            match tokio::time::timeout(Duration::from_secs(5), channel_clone.recv()).await {
                Ok(Ok(message)) => {
                    if let Some(frame) = message.last() {
                        if let Ok(response) = String::from_utf8(frame.clone()) {
                            let parts: Vec<&str> = response.split(':').collect();
                            if parts.len() == 2 {
                                if let (Ok(req_id), Ok(result)) =
                                    (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                                {
                                    let mut p = pending_clone.lock().await;
                                    if let Some((n, start)) = p.remove(&req_id) {
                                        let rtt = start.elapsed().as_micros();
                                        info!(
                                            "[RES {}] <-- fib({}) = {} | rtt={}us",
                                            req_id, n, result, rtt
                                        );
                                        results.push(rtt);
                                    }
                                }
                            }
                            count += 1;
                        }
                    }
                }
                _ => break,
            }
        }
        results
    });

    for s in senders {
        let _ = s.await;
    }

    let latencies = receiver.await.unwrap_or_default();
    let total_time = start_time.elapsed();

    if !latencies.is_empty() {
        let mut sorted = latencies.clone();
        sorted.sort();
        let sum: u128 = sorted.iter().sum();
        let mean = sum / sorted.len() as u128;
        let p50 = sorted[sorted.len() / 2];
        let p99 = sorted[((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1)];

        let throughput = sorted.len() as f64 / total_time.as_secs_f64();

        info!("---");
        info!("Requests:   {}", sorted.len());
        info!("Total time: {:.2}s", total_time.as_secs_f64());
        info!("Throughput: {:.0} req/s", throughput);
        info!("Latency:    mean={}us p50={}us p99={}us", mean, p50, p99);
    }

    channel.shutdown().await;
    Ok(())
}
