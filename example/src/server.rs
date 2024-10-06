use env_logger;
use log::{debug, error, info};
use threadsafe_zmq::{ChannelPair, Sender, ZmqByteStream};
use zmq::Context;

fn main() {
    env_logger::init();

    let addr = "tcp://*:5555";
    let ctx = Context::new();
    let socket = ctx
        .socket(zmq::ROUTER)
        .expect("Failed to create ROUTER socket");
    socket.bind(addr).expect("Failed to bind to address");

    let channel_pair = ChannelPair::new(socket).expect("Failed to create channel pair");
    info!("Server listening on {}", addr);

    loop {
        debug!("Waiting to receive messages...");

        match channel_pair.rx_chan().recv() {
            Ok(message) => {
                if message.len() < 2 {
                    error!("Received malformed message: {:?}", message);
                    continue;
                }

                println!("Received message: {:?}", message);
                let cp = channel_pair.clone();
                std::thread::spawn(move || {
                    calculate_fib(message, cp.tx_chan());
                });
            }
            Err(rcv_err) => {
                error!("Failed to receive message: {:?}", rcv_err);
            }
        }
    }
}

fn calculate_fib(messages: ZmqByteStream, sender: &Sender) {
    // The first part is the identity, and the second part is the actual message
    let identity = messages[0].clone();
    let payload = messages[1].clone();

    let id_str = String::from_utf8_lossy(&identity);

    if payload.is_empty() {
        error!("Received an empty payload, skipping Fibonacci calculation.");
        return;
    }

    info!("Received message from: {:?}", id_str);

    // Deserialize the message into u32
    // let number = match payload.as_slice().try_into() {
    //     Ok(bytes) => u32::from_le_bytes(bytes),
    //     Err(_) => {
    //         error!("Failed to deserialize payload, skipping.");
    //         return;
    //     }
    // };
    let number = 13;

    info!("Calculating Fibonacci for number: {}", number);
    let result = fibonacci_recursive(number);
    let result_bytes = result.to_le_bytes().to_vec();

    // The response must include the identity frame, followed by the result
    let response = vec![identity.clone(), result_bytes];
    match sender.send(response) {
        Ok(_) => info!("Successfully sent response: {:?} to: {:?}", result, id_str),
        Err(err) => error!("Failed to send response: {:?}", err),
    }
}

fn fibonacci_recursive(n: u32) -> u32 {
    if n <= 1 {
        n
    } else {
        fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2)
    }
}
