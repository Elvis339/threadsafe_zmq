use env_logger;
use log::{error, info};
use std::sync::Arc;
use threadsafe_zmq::{ChannelPair, ZmqByteStream};
use tokio::task;
use zmq::Context;

mod utils;
use crate::utils::to_string;

#[tokio::main]
async fn main() {
    env_logger::init();

    let addr = "tcp://*:5555";
    let ctx = Context::new();
    let socket = ctx
        .socket(zmq::ROUTER)
        .expect("Failed to create ROUTER socket");
    socket.bind(addr).expect("Failed to bind to address");

    let channel_pair = ChannelPair::new(&ctx, socket).expect("Failed to create channel pair");
    info!("Server listening on {}", addr);
    loop {
        let channel_pair_clone = Arc::clone(&channel_pair);
        match task::spawn_blocking(move || channel_pair_clone.rx().recv()).await {
            Ok(Ok(message)) => {
                let channel_pair_for_task = Arc::clone(&channel_pair);
                task::spawn(handle_message(message, channel_pair_for_task));
            }
            Ok(Err(e)) => {
                error!("Failed to receive message: {:?}", e);
                break;
            }
            Err(e) => {
                error!("Task join error: {:?}", e);
                break;
            }
        }
    }
}

async fn handle_message(messages: ZmqByteStream, channel_pair: Arc<ChannelPair>) {
    let identity = messages[0].clone();

    if let Ok(str_num) = String::from_utf8(messages.last().unwrap().clone()) {
        if let Ok(number) = str_num.parse::<u64>() {
            let result = task::spawn_blocking(move || fibonacci(number)).await;

            match result {
                Ok(result) => {
                    let result_bytes = to_string(result);
                    if let Err(err) = channel_pair
                        .tx()
                        .send(vec![identity.clone(), result_bytes.as_bytes().to_vec()])
                    {
                        error!("Failed to send response: {:?}", err);
                    } else {
                        info!("SND: fib({})={}", number, result);
                    }
                }
                Err(e) => {
                    error!("Task join error while calculating fibonacci({}): {:?}", number, e);
                }
            }
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
        let temp = a + b;
        a = b;
        b = temp;
    }

    b
}
