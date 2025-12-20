use clap::{Parser, ValueEnum};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use threadsafe_zmq::ChannelPair;
use zmq::Context;

#[cfg(feature = "async")]
use threadsafe_zmq::AsyncChannelPair;

#[derive(Parser)]
#[command(name = "bench", about = "Benchmark threadsafe_zmq")]
struct Args {
    #[arg(short, long, default_value = "all")]
    mode: Mode,

    #[arg(short, long, default_value_t = 10)]
    duration_secs: u64,

    #[arg(short, long, default_value_t = 8)]
    workers: usize,

    #[arg(short = 'c', long, default_value_t = 30)]
    work_complexity: u64,
}

#[derive(Clone, ValueEnum, PartialEq)]
enum Mode {
    Throughput,
    Latency,
    All,
}

static ADDR_INDEX: AtomicU64 = AtomicU64::new(0);

fn unique_addr() -> String {
    let id = ADDR_INDEX.fetch_add(1, Ordering::SeqCst);
    format!("inproc://bench-{}", id)
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn do_work(n: u64) -> u64 {
    if n <= 1 {
        return n;
    }
    let mut a: u64 = 0;
    let mut b: u64 = 1;
    for _ in 2..=n {
        let temp = a.wrapping_add(b);
        a = b;
        b = temp;
    }
    b
}

fn run_raw_single_producer(args: &Args) -> (u64, f64) {
    let ctx = Context::new();
    let addr = unique_addr();

    let server = ctx.socket(zmq::PAIR).unwrap();
    server.bind(&addr).unwrap();

    let client = ctx.socket(zmq::PAIR).unwrap();
    client.connect(&addr).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let work = args.work_complexity;
    let duration_secs = args.duration_secs;

    let running_clone = Arc::clone(&running);
    let consumer = thread::spawn(move || loop {
        match server.recv_multipart(zmq::DONTWAIT) {
            Ok(m) if m[0] == b"STOP" => break,
            Ok(_) => {
                do_work(work);
            }
            Err(zmq::Error::EAGAIN) => {
                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }
                thread::yield_now();
            }
            Err(_) => break,
        }
    });

    let msg = vec![b"REQ".to_vec()];
    let duration = Duration::from_secs(duration_secs);
    let start = Instant::now();
    let mut count = 0u64;

    while start.elapsed() < duration {
        client.send_multipart(&msg, 0).unwrap();
        count += 1;
    }

    running.store(false, Ordering::Relaxed);
    client.send_multipart(&[b"STOP".to_vec()], 0).unwrap();
    let _ = consumer.join();

    let throughput = count as f64 / duration_secs as f64;
    (count, throughput)
}

// THE PROBLEM: Raw ZMQ with mutex (N producers sharing socket via lock)
// This is what you have to do without threadsafe_zmq
fn run_raw_mutex(args: &Args) -> (u64, f64) {
    let ctx = Context::new();
    let addr = unique_addr();

    let server = ctx.socket(zmq::PAIR).unwrap();
    server.bind(&addr).unwrap();

    let client = ctx.socket(zmq::PAIR).unwrap();
    client.connect(&addr).unwrap();

    let client = Arc::new(Mutex::new(client));

    let running = Arc::new(AtomicBool::new(true));
    let sent = Arc::new(AtomicU64::new(0));
    let work = args.work_complexity;
    let duration_secs = args.duration_secs;
    let num_producers = args.workers;

    let running_clone = Arc::clone(&running);
    let consumer = thread::spawn(move || loop {
        match server.recv_multipart(zmq::DONTWAIT) {
            Ok(m) if m[0] == b"STOP" => break,
            Ok(_) => {
                do_work(work);
            }
            Err(zmq::Error::EAGAIN) => {
                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }
                thread::yield_now();
            }
            Err(_) => break,
        }
    });

    let mut producers = Vec::new();
    for _ in 0..num_producers {
        let client = Arc::clone(&client);
        let running = Arc::clone(&running);
        let sent = Arc::clone(&sent);

        producers.push(thread::spawn(move || {
            let msg = vec![b"REQ".to_vec()];
            let duration = Duration::from_secs(duration_secs);
            let start = Instant::now();

            while start.elapsed() < duration && running.load(Ordering::Relaxed) {
                // Lock, send, unlock - this serializes all producers
                let guard = client.lock().unwrap();
                if guard.send_multipart(&msg, 0).is_ok() {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
                drop(guard);
            }
        }));
    }

    for p in producers {
        let _ = p.join();
    }

    running.store(false, Ordering::Relaxed);
    {
        let guard = client.lock().unwrap();
        let _ = guard.send_multipart(&[b"STOP".to_vec()], 0);
    }
    let _ = consumer.join();

    let total = sent.load(Ordering::Relaxed);
    let throughput = total as f64 / duration_secs as f64;
    (total, throughput)
}

// ALTERNATIVE: ZMQ Proxy pattern (PUSH/PULL through broker)
// Each producer has own socket, broker forwards to worker
fn run_zmq_proxy(args: &Args) -> (u64, f64) {
    let ctx = Context::new();
    let frontend_addr = unique_addr();
    let backend_addr = unique_addr();

    let running = Arc::new(AtomicBool::new(true));
    let sent = Arc::new(AtomicU64::new(0));
    let work = args.work_complexity;
    let duration_secs = args.duration_secs;
    let num_producers = args.workers;

    // Broker thread: PULL from producers, PUSH to worker
    let ctx_clone = ctx.clone();
    let frontend = frontend_addr.clone();
    let backend = backend_addr.clone();
    let running_broker = Arc::clone(&running);
    let broker = thread::spawn(move || {
        let pull = ctx_clone.socket(zmq::PULL).unwrap();
        pull.bind(&frontend).unwrap();

        let push = ctx_clone.socket(zmq::PUSH).unwrap();
        push.bind(&backend).unwrap();

        while running_broker.load(Ordering::Relaxed) {
            match pull.recv_multipart(zmq::DONTWAIT) {
                Ok(msg) => {
                    let _ = push.send_multipart(&msg, 0);
                }
                Err(zmq::Error::EAGAIN) => {
                    thread::yield_now();
                }
                Err(_) => break,
            }
        }
    });

    // Worker thread: receives from broker
    let ctx_clone = ctx.clone();
    let backend = backend_addr.clone();
    let running_worker = Arc::clone(&running);
    let worker = thread::spawn(move || {
        let pull = ctx_clone.socket(zmq::PULL).unwrap();
        pull.connect(&backend).unwrap();

        while running_worker.load(Ordering::Relaxed) {
            match pull.recv_multipart(zmq::DONTWAIT) {
                Ok(_) => {
                    do_work(work);
                }
                Err(zmq::Error::EAGAIN) => {
                    thread::yield_now();
                }
                Err(_) => break,
            }
        }
    });

    thread::sleep(Duration::from_millis(10));

    // N producer threads, each with own PUSH socket
    let mut producers = Vec::new();
    for _ in 0..num_producers {
        let ctx = ctx.clone();
        let addr = frontend_addr.clone();
        let running = Arc::clone(&running);
        let sent = Arc::clone(&sent);

        producers.push(thread::spawn(move || {
            let push = ctx.socket(zmq::PUSH).unwrap();
            push.connect(&addr).unwrap();

            let msg = vec![b"REQ".to_vec()];
            let duration = Duration::from_secs(duration_secs);
            let start = Instant::now();

            while start.elapsed() < duration && running.load(Ordering::Relaxed) {
                if push.send_multipart(&msg, 0).is_ok() {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for p in producers {
        let _ = p.join();
    }

    running.store(false, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(50));
    let _ = broker.join();
    let _ = worker.join();

    let total = sent.load(Ordering::Relaxed);
    let throughput = total as f64 / duration_secs as f64;
    (total, throughput)
}

// THE SOLUTION: ChannelPair (N producers, thread-safe without mutex contention)
fn run_channelpair(args: &Args) -> (u64, f64) {
    let ctx = Context::new();
    let addr = unique_addr();

    let server_sock = ctx.socket(zmq::PAIR).unwrap();
    server_sock.bind(&addr).unwrap();

    let client_sock = ctx.socket(zmq::PAIR).unwrap();
    client_sock.connect(&addr).unwrap();

    let server = ChannelPair::new(&ctx, server_sock).unwrap();
    let client = ChannelPair::new(&ctx, client_sock).unwrap();

    thread::sleep(Duration::from_millis(10));

    let running = Arc::new(AtomicBool::new(true));
    let sent = Arc::new(AtomicU64::new(0));
    let work = args.work_complexity;
    let duration_secs = args.duration_secs;
    let num_producers = args.workers;

    let server_clone = Arc::clone(&server);
    let running_clone = Arc::clone(&running);
    let consumer = thread::spawn(move || loop {
        match server_clone.recv_timeout(Duration::from_millis(100)) {
            Ok(m) if m[0] == b"STOP" => break,
            Ok(_) => {
                do_work(work);
            }
            Err(_) => {
                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    });

    let mut producers = Vec::new();
    for _ in 0..num_producers {
        let client = Arc::clone(&client);
        let running = Arc::clone(&running);
        let sent = Arc::clone(&sent);

        producers.push(thread::spawn(move || {
            let msg = vec![b"REQ".to_vec()];
            let duration = Duration::from_secs(duration_secs);
            let start = Instant::now();

            while start.elapsed() < duration && running.load(Ordering::Relaxed) {
                if client.send(msg.clone()).is_ok() {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for p in producers {
        let _ = p.join();
    }

    running.store(false, Ordering::Relaxed);
    let _ = client.send(vec![b"STOP".to_vec()]);
    let _ = consumer.join();

    server.shutdown();
    client.shutdown();

    let total = sent.load(Ordering::Relaxed);
    let throughput = total as f64 / duration_secs as f64;
    (total, throughput)
}

// Async: Multiple producers via AsyncChannelPair
#[cfg(feature = "async")]
fn run_async_channelpair(args: &Args) -> (u64, f64) {
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();
    let duration_secs = args.duration_secs;
    let num_producers = args.workers;
    let work = args.work_complexity;

    rt.block_on(async {
        let ctx = Context::new();
        let addr = unique_addr();

        let server_sock = ctx.socket(zmq::PAIR).unwrap();
        server_sock.bind(&addr).unwrap();

        let client_sock = ctx.socket(zmq::PAIR).unwrap();
        client_sock.connect(&addr).unwrap();

        let server = Arc::new(AsyncChannelPair::new(&ctx, server_sock).unwrap());
        let client = Arc::new(AsyncChannelPair::new(&ctx, client_sock).unwrap());

        tokio::time::sleep(Duration::from_millis(10)).await;

        let running = Arc::new(AtomicBool::new(true));
        let sent = Arc::new(AtomicU64::new(0));

        let server_clone = Arc::clone(&server);
        let running_clone = Arc::clone(&running);
        let consumer = tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(100), server_clone.recv()).await {
                    Ok(Ok(m)) if m[0] == b"STOP" => break,
                    Ok(Ok(_)) => {
                        do_work(work);
                    }
                    _ => {
                        if !running_clone.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        });

        let mut producers = Vec::new();
        for _ in 0..num_producers {
            let client = Arc::clone(&client);
            let running = Arc::clone(&running);
            let sent = Arc::clone(&sent);

            producers.push(tokio::spawn(async move {
                let msg = vec![b"REQ".to_vec()];
                let duration = Duration::from_secs(duration_secs);
                let start = Instant::now();

                while start.elapsed() < duration && running.load(Ordering::Relaxed) {
                    if client.send(msg.clone()).await.is_ok() {
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for p in producers {
            let _ = p.await;
        }

        running.store(false, Ordering::Relaxed);
        let _ = client.send(vec![b"STOP".to_vec()]).await;
        let _ = consumer.await;

        server.shutdown().await;
        client.shutdown().await;

        let total = sent.load(Ordering::Relaxed);
        let throughput = total as f64 / duration_secs as f64;
        (total, throughput)
    })
}

// Latency: round-trip through ChannelPair
fn run_latency(args: &Args) -> Vec<u64> {
    let ctx = Context::new();
    let addr = unique_addr();

    let server_sock = ctx.socket(zmq::PAIR).unwrap();
    server_sock.bind(&addr).unwrap();

    let client_sock = ctx.socket(zmq::PAIR).unwrap();
    client_sock.connect(&addr).unwrap();

    let server = ChannelPair::new(&ctx, server_sock).unwrap();
    let client = ChannelPair::new(&ctx, client_sock).unwrap();

    thread::sleep(Duration::from_millis(10));

    let running = Arc::new(AtomicBool::new(true));
    let work = args.work_complexity;

    let server_clone = Arc::clone(&server);
    let running_clone = Arc::clone(&running);
    let worker = thread::spawn(move || loop {
        match server_clone.recv_timeout(Duration::from_millis(100)) {
            Ok(m) if m[0] == b"STOP" => break,
            Ok(_) => {
                do_work(work);
                let _ = server_clone.send(vec![b"OK".to_vec()]);
            }
            Err(_) => {
                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    });

    let mut latencies = Vec::new();
    let duration = Duration::from_secs(args.duration_secs);
    let start = Instant::now();
    let msg = vec![b"REQ".to_vec()];

    while start.elapsed() < duration {
        let req_start = Instant::now();
        client.send(msg.clone()).unwrap();
        let _ = client.recv_timeout(Duration::from_secs(5)).unwrap();
        latencies.push(req_start.elapsed().as_micros() as u64);
    }

    running.store(false, Ordering::Relaxed);
    let _ = client.send(vec![b"STOP".to_vec()]);
    let _ = worker.join();

    server.shutdown();
    client.shutdown();

    latencies
}

fn print_latency_stats(latencies: &mut Vec<u64>) {
    if latencies.is_empty() {
        return;
    }

    latencies.sort();
    let n = latencies.len();
    let sum: u64 = latencies.iter().sum();
    let mean = sum / n as u64;
    let p50 = latencies[n / 2];
    let p95 = latencies[(n as f64 * 0.95) as usize];
    let p99 = latencies[((n as f64 * 0.99) as usize).min(n - 1)];
    let min = latencies[0];
    let max = latencies[n - 1];

    println!("requests: {}", fmt_num(n as u64));
    println!("min:      {} us", fmt_num(min));
    println!("mean:     {} us", fmt_num(mean));
    println!("p50:      {} us", fmt_num(p50));
    println!("p95:      {} us", fmt_num(p95));
    println!("p99:      {} us", fmt_num(p99));
    println!("max:      {} us", fmt_num(max));
}

fn main() {
    let args = Args::parse();

    println!("threadsafe_zmq benchmark");
    println!("========================");
    println!("duration:   {}s", args.duration_secs);
    println!("producers:  {}", args.workers);
    println!("work:       fib({})", args.work_complexity);
    println!();

    match args.mode {
        Mode::Throughput | Mode::All => {
            println!("Throughput\n");

            // Baseline: what one thread can do
            println!("Raw ZMQ (1 producer, baseline)...");
            let (raw_total, raw_tps) = run_raw_single_producer(&args);
            println!(
                "{} msg/s ({} total)\n",
                fmt_num(raw_tps as u64),
                fmt_num(raw_total)
            );

            // Problem 1: mutex contention when sharing socket
            println!("Raw ZMQ + Mutex ({} producers)...", args.workers);
            let (mutex_total, mutex_tps) = run_raw_mutex(&args);
            println!(
                "{} msg/s ({} total)",
                fmt_num(mutex_tps as u64),
                fmt_num(mutex_total)
            );
            println!("vs baseline: {:.2}x\n", mutex_tps / raw_tps);

            // Problem 2: proxy pattern adds overhead
            println!("ZMQ Proxy ({} producers, PUSH/PULL)...", args.workers);
            let (proxy_total, proxy_tps) = run_zmq_proxy(&args);
            println!(
                "{} msg/s ({} total)",
                fmt_num(proxy_tps as u64),
                fmt_num(proxy_total)
            );
            println!("vs baseline: {:.2}x\n", proxy_tps / raw_tps);

            // Our solution
            println!("ChannelPair ({} producers)...", args.workers);
            let (cp_total, cp_tps) = run_channelpair(&args);
            println!(
                "{} msg/s ({} total)",
                fmt_num(cp_tps as u64),
                fmt_num(cp_total)
            );
            println!("vs baseline: {:.2}x", cp_tps / raw_tps);
            println!("vs mutex:    {:.2}x", cp_tps / mutex_tps);
            println!("vs proxy:    {:.2}x\n", cp_tps / proxy_tps);

            #[cfg(feature = "async")]
            {
                println!("AsyncChannelPair ({} producers)...", args.workers);
                let (async_total, async_tps) = run_async_channelpair(&args);
                println!(
                    "{} msg/s ({} total)",
                    fmt_num(async_tps as u64),
                    fmt_num(async_total)
                );
                println!("vs baseline: {:.2}x\n", async_tps / raw_tps);
            }
        }
        _ => {}
    }

    match args.mode {
        Mode::Latency | Mode::All => {
            println!("Latency (round-trip)\n");
            let mut latencies = run_latency(&args);
            print_latency_stats(&mut latencies);
        }
        _ => {}
    }

    println!();
}
