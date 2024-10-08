use crate::utils::to_string;
use log::{error, info};
use rand::Rng;
use zmq::Context;

mod utils;

fn main() {
    env_logger::init();

    let addr = "tcp://localhost:5555";
    let ctx = Context::new();
    let socket = ctx
        .socket(zmq::DEALER)
        .expect("Failed to create DEALER socket");

    let id = format!("Client-{}", 1);
    socket
        .set_identity(id.clone().as_bytes())
        .expect("Failed to set identity");
    socket.connect(addr).expect("Failed to connect to server");

    info!("Connected to: {}", addr);
    loop {
        let rand_num = to_string(generate_random_number());

        // Send the message to the server
        match socket.send(rand_num.as_bytes(), 0) {
            Ok(_) => {
                info!("SND: fib({})=?", rand_num.clone());
            }
            Err(snd_err) => {
                error!("SND: failed to send message: {:?}", snd_err);
                continue;
            }
        }

        // Receive the response from the server
        match socket.recv_multipart(0) {
            Ok(message) => {
                for (_, frame) in message.iter().enumerate() {
                    match String::from_utf8(frame.clone()) {
                        Ok(str_frame) => {
                            if str_frame.is_empty() {
                                continue;
                            }

                            info!("RCV: fib({})={}", rand_num, str_frame);
                            info!("--------------------------------------------------")
                        }
                        Err(e) => {
                            error!("RCV: failed to convert frame to string: {:?}", e);
                        }
                    }
                }
            }
            Err(rcv_err) => {
                error!("RCV: failed to receive message: {:?}", rcv_err);
            }
        }
    }
}

fn generate_random_number() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(1..=80)
}
