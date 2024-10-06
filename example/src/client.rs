use env_logger;
use log::{error, info};
use rand::Rng;
use zmq::Context;

fn main() {
    env_logger::init();

    let clients = 4;
    let mut handles = Vec::with_capacity(clients);

    for i in 0..clients {
        let client_id = i;
        let handle = std::thread::spawn(move || {
            let addr = "tcp://localhost:5555";
            let ctx = Context::new();
            let socket = ctx
                .socket(zmq::DEALER)
                .expect("Failed to create PAIR socket");

            let rand_id = client_id as u8 + generate_random_number();
            let id = format!("client-{}", rand_id);
            socket
                .set_identity(id.clone().as_bytes())
                .expect("Failed to set identity");
            socket.connect(addr).expect("Failed to connect to server");

            info!("{} connected to: {}", id, addr);
            loop {
                let rand_num = generate_random_number();
                let rand_num_bytes = rand_num.to_le_bytes().to_vec();

                match socket.send_multipart(vec![rand_num_bytes], 0) {
                    Ok(_) => info!("{}, sent number: {}", id, rand_num),
                    Err(snd_err) => {
                        error!("{}, failed to send message: {:?}", id, snd_err);
                        continue;
                    }
                }

                match socket.recv_multipart(0) {
                    Ok(message) => {
                        info!("Client {}, received result: {:?}", client_id, message);
                    }
                    Err(rcv_err) => {
                        error!(
                            "Client {}, failed to receive message: {:?}",
                            client_id, rcv_err
                        );
                    }
                }

                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });
        handles.push(handle);
    }

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn generate_random_number() -> u8 {
    let mut rng = rand::thread_rng();
    rng.gen_range(0..=30)
}
