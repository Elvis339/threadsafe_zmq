use anyhow::anyhow;
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use zmq::{Context, PollEvents, Socket, SocketType};

static UNIQUE_INDEX: AtomicU16 = AtomicU16::new(0);
const IN: usize = 0;
const OUT: usize = 1;
const DEFAULT_LINGER_TIME: i32 = 30;

type ZmqByteStream = Vec<Vec<u8>>;

struct ChannelPair {
    ctx: Context,
    z_sock: Socket,
    z_tx: Vec<Socket>,
    z_control: Vec<Socket>,
    tx_chan: Sender<ZmqByteStream>,
    rx_chan: Receiver<ZmqByteStream>,
    error_chan: (Sender<anyhow::Error>, Receiver<anyhow::Error>),
    control_chan: (Sender<bool>, Receiver<bool>),
}

impl Clone for ChannelPair {
    // Avoid calling clone!!!
    fn clone(&self) -> Self {
        let socket_type = self.z_sock.get_socket_type().unwrap();
        let last_endpoint = self.z_sock.get_last_endpoint().unwrap().unwrap();
        Self::disconnect_socket(&self.z_sock).unwrap();

        for z_tx_socket in self.z_tx.iter() {
            Self::disconnect_socket(z_tx_socket).unwrap();
        }

        for z_control_socket in self.z_control.iter() {
            Self::disconnect_socket(z_control_socket).unwrap();
        }

        let z_tx = Self::new_pair(&self.ctx).unwrap();
        let z_control = Self::new_pair(&self.ctx).unwrap();

        let new_socket = self.ctx.socket(socket_type).unwrap();

        new_socket.set_rcvtimeo(0).unwrap();
        new_socket.set_sndtimeo(0).unwrap();

        match socket_type {
            SocketType::PUB
            | SocketType::REP
            | SocketType::PUSH
            | SocketType::XPUB
            | SocketType::ROUTER => {
                new_socket.bind(&last_endpoint).unwrap();
            }
            SocketType::SUB
            | SocketType::REQ
            | SocketType::DEALER
            | SocketType::PULL
            | SocketType::XSUB
            | SocketType::STREAM => {
                new_socket.connect(&last_endpoint).unwrap();
            }
            // Special handling for PAIR socket (try bind first, then connect if it fails)
            SocketType::PAIR => match new_socket.bind(&last_endpoint) {
                Ok(_) => {}
                Err(_) => {
                    new_socket.connect(&last_endpoint).unwrap();
                }
            },
        }

        let mut cp = Self {
            ctx: self.ctx.clone(),
            z_sock: new_socket,
            tx_chan: self.tx_chan.clone(),
            rx_chan: self.rx_chan.clone(),
            error_chan: self.error_chan.clone(),
            control_chan: self.control_chan.clone(),
            z_tx,
            z_control,
        };

        Self::configure_socket(&mut cp).unwrap();

        cp
    }
}

unsafe impl Send for ChannelPair {}
unsafe impl Sync for ChannelPair {}

impl ChannelPair {
    pub fn new(ctx: Context, socket: Socket) -> anyhow::Result<Arc<Self>> {
        let z_tx = Self::new_pair(&ctx)?;
        let z_control = Self::new_pair(&ctx)?;

        let (tx_chan, rx_chan) = unbounded::<ZmqByteStream>();

        let mut channel_pair = Self {
            ctx,
            z_tx,
            z_control,
            tx_chan,
            rx_chan,
            z_sock: socket,
            error_chan: unbounded::<anyhow::Error>(),
            control_chan: unbounded::<bool>(),
        };
        Self::configure_socket(&mut channel_pair)?;

        // let tmp = channel_pair.clone();
        let channel_pair = Arc::new(channel_pair);

        // === run sockets ==={
        let channel_pair_clone = Arc::clone(&channel_pair);
        std::thread::spawn(move || channel_pair_clone.run_sockets());

        // === run channels ===
        let channel_pair_clone = Arc::clone(&channel_pair);
        std::thread::spawn(move || channel_pair_clone.run_channels());

        Ok(channel_pair)
    }

    fn run_sockets(&self) {
        let mut to_xmit: ZmqByteStream = vec![];

        // Create poll items array.
        let mut items = [
            self.z_sock.as_poll_item(PollEvents::empty()),
            self.z_tx[OUT].as_poll_item(PollEvents::empty()),
            self.z_control[OUT].as_poll_item(zmq::POLLIN),
        ];

        loop {
            let mut z_sock_flag = PollEvents::empty();
            let mut tx_sock_flag = PollEvents::empty();

            if to_xmit.is_empty() {
                tx_sock_flag |= zmq::POLLIN;
            } else {
                z_sock_flag |= zmq::POLLOUT;
            }

            items[0].set_events(z_sock_flag);
            items[1].set_events(tx_sock_flag);

            // Pass the whole `items` array as a mutable reference.
            match zmq::poll(&mut items, -1) {
                Ok(_) => {
                    if items[0].is_readable() {
                        match self.z_sock.recv_multipart(0) {
                            Ok(zmq_byte_stream) => self.tx_chan().send(zmq_byte_stream).unwrap(),
                            Err(recv_err) => {
                                self.on_err(anyhow!(
                                    "Failed to receive message on the socket: {:?}",
                                    recv_err
                                ));
                                return;
                            }
                        }
                    }

                    if items[0].is_writable() && !to_xmit.is_empty() {
                        match self.z_sock.send_multipart(to_xmit.clone(), 0) {
                            Ok(_) => {
                                to_xmit = vec![];
                            }
                            Err(snd_err) => {
                                self.on_err(anyhow!(
                                    "Failed to send message on the socket: {:?}",
                                    snd_err
                                ));
                                return;
                            }
                        }
                    }

                    if items[1].is_readable() && to_xmit.is_empty() {
                        match self.z_tx[OUT].recv_multipart(0) {
                            Ok(zmq_byte_stream) => {
                                to_xmit = zmq_byte_stream;
                            }
                            Err(z_tx_recv_err) => {
                                self.on_err(anyhow!(
                                    "Failed to receive message on the tx socket: {:?}",
                                    z_tx_recv_err
                                ));
                                return;
                            }
                        }
                    }

                    if items[2].is_writable() {
                        match self.z_control[OUT].recv_multipart(0) {
                            Ok(_) => {}
                            Err(ctrl_err) => {
                                self.on_err(anyhow!(
                                    "Failed to receive message on the control socket: {:?}",
                                    ctrl_err
                                ));
                                return;
                            }
                        }

                        let linger = self.z_sock.get_linger().unwrap_or(DEFAULT_LINGER_TIME);
                        if let Err(err) = self.z_sock.set_linger(linger) {
                            self.on_err(anyhow!("Failed to set linger on the socket: {:?}", err));
                            return;
                        }

                        if !to_xmit.is_empty() {
                            match self.z_sock.send_multipart(to_xmit.clone(), 0) {
                                Ok(_) => {}
                                Err(snd_err) => {
                                    self.on_err(anyhow!(
                                        "Failed to send pending message on the socket: {:?}",
                                        snd_err
                                    ));
                                    return;
                                }
                            }
                        } else {
                            to_xmit = vec![];
                        }

                        items[2].set_events(PollEvents::empty());
                        items[0].set_events(PollEvents::empty());
                        items[1].set_events(zmq::POLLIN);
                    }
                }
                Err(poll_err) => {
                    self.on_err(anyhow!("Polling error: {:?}", poll_err));
                    return;
                }
            }
        }
    }

    fn run_channels(&self) {
        // Run indefinitely until channels are closed or an error occurs.
        loop {
            select! {
                recv(self.rx_chan()) -> msg => {
                    match msg {
                        Ok(msg) => {
                            // If a message is received, try to send it through zTx.
                            if let Err(err) = self.z_tx[IN].send_multipart(&msg, 0) {
                                self.on_err(anyhow!("Failed to send message on tx socket: {:?}", err));
                                return;
                            }
                        },
                        Err(_) => {
                            self.on_err(anyhow!("ZMQ tx channel closed unexpectedly"));
                            return;
                        }
                    }
                },
                recv(self.rx_control_chan()) -> control => {
                    match control {
                        Ok(control) => {
                            if control {
                                // Send an empty message as a control signal.
                                if let Err(err) = self.z_control[IN].send("", 0) {
                                    self.on_err(anyhow!("Failed to send message on the control socket: {:?}", err));
                                }
                            }
                            return;
                        },
                        Err(_) => {
                            self.on_err(anyhow!("ZMQ control channel closed unexpectedly"));
                            return;
                        }
                    }
                }
            }
        }
    }

    pub fn rx_chan(&self) -> &Receiver<ZmqByteStream> {
        &self.rx_chan
    }

    pub fn tx_chan(&self) -> &Sender<ZmqByteStream> {
        &self.tx_chan
    }

    fn tx_control_chan(&self) -> &Sender<bool> {
        &self.control_chan.0
    }

    fn rx_control_chan(&self) -> &Receiver<bool> {
        &self.control_chan.1
    }

    fn tx_err_chan(&self) -> &Sender<anyhow::Error> {
        &self.error_chan.0
    }

    fn on_err(&self, error: anyhow::Error) {
        let _ = self.tx_err_chan().send(error);
        let _ = self.tx_control_chan().send(false);
    }

    fn configure_socket(&mut self) -> anyhow::Result<()> {
        self.z_sock.set_rcvtimeo(0)?;
        self.z_sock.set_sndtimeo(0)?;

        for socket in &self.z_tx {
            socket.set_rcvtimeo(0)?;
            socket.set_sndtimeo(0)?;
        }

        for socket in &self.z_control {
            socket.set_rcvtimeo(0)?;
            socket.set_sndtimeo(0)?;
        }

        Ok(())
    }

    fn new_pair(context: &Context) -> anyhow::Result<Vec<Socket>> {
        let addr = format!("inproc://_channelpair_internal-{}", get_unique_id());
        let server_pair = context.socket(zmq::PAIR)?;
        server_pair.bind(&addr)?;

        let client_pair = context.socket(zmq::PAIR)?;
        client_pair.connect(&addr)?;

        Ok(vec![server_pair, client_pair])
    }

    fn disconnect_socket(socket: &Socket) -> Result<(), zmq::Error> {
        // Retrieve the last endpoint, handling both the outer and inner Result.
        match socket.get_last_endpoint()? {
            // Handle the case where the endpoint is a valid UTF-8 string.
            Ok(endpoint) => socket.disconnect(&endpoint),

            // Handle the case where the endpoint is raw bytes (non-UTF-8).
            Err(bytes) => {
                let endpoint = String::from_utf8_lossy(&bytes).into_owned();
                socket.disconnect(&endpoint)
            }
        }
    }
}

fn get_unique_id() -> u16 {
    UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst)
}

// // Helper function to create a new pair socket.
// fn new_pair_socket() -> (Socket, Context, Socket, Context) {
//     let ctx1 = Context::new();
//     let socket1 = ctx1.socket(zmq::PAIR).unwrap();
//     socket1.bind("tcp://127.0.0.1:9737").unwrap();
//
//     let ctx2 = Context::new();
//     let socket2 = ctx2.socket(zmq::PAIR).unwrap();
//     socket2.connect("tcp://127.0.0.1:9737").unwrap();
//
//     (socket1, ctx1, socket2, ctx2)
// }
//
// // Helper function to compare two ZMQ byte streams.
// fn msg_equal(a: &ZmqByteStream, b: &ZmqByteStream) -> bool {
//     if a.len() != b.len() {
//         return false;
//     }
//     for (i, msg) in a.iter().enumerate() {
//         if msg.len() != b[i].len() {
//             return false;
//         }
//         for (j, byte) in msg.iter().enumerate() {
//             if *byte != b[i][j] {
//                 return false;
//             }
//         }
//     }
//     true
// }
//
// // Helper function to run the echo logic.
// fn run_echo(num: usize, c: &ChannelPair) {
//     let mut remaining = num;
//
//     while remaining > 0 {
//         select! {
//             recv(c.rx_chan()) -> msg => {
//                 match msg {
//                     Ok(msg) => {
//                         // Send the received message back to the Tx channel.
//                         c.tx_chan().send(msg).expect("Failed to send message");
//                         remaining -= 1;
//
//                         if remaining <= 0 {
//                             println!("ECHO: done");
//                             return;
//                         }
//                     },
//                     Err(_) => {
//                         panic!("Cannot read from echo channel");
//                     }
//                 }
//             },
//             default(Duration::from_secs(5)) => {
//                 panic!("Timeout in run_echo");
//             }
//         }
//     }
// }
//
// // Helper function to run the write logic.
// fn run_write(num: usize, c: &ChannelPair) {
//     let mut tx = 0;
//     let mut rx = 0;
//     let src_msg: ZmqByteStream = vec![b"Hello".to_vec(), b"World".to_vec()];
//     let mut txchan = Some(c.tx_chan());
//
//     while rx < num {
//         select! {
//             recv(c.rx_chan()) -> msg => {
//                 match msg {
//                     Ok(received_msg) => {
//                         rx += 1;
//                         if !msg_equal(&received_msg, &src_msg) {
//                             panic!("Messages do not match");
//                         }
//                         if rx >= num {
//                             println!("MAIN: done");
//                             return;
//                         }
//                     },
//                     Err(_) => {
//                         panic!("Cannot read from main channel");
//                     }
//                 }
//             },
//             send(txchan.unwrap_or(&c.tx_chan()), src_msg.clone()) -> result => {
//                 if let Ok(_) = result {
//                     tx += 1;
//                     if tx >= num {
//                         txchan = None; // Disable the txchan (like setting to `nil` in Go)
//                     }
//                 }
//             },
//             default(Duration::from_secs(1)) => {
//                 panic!("Timeout in runWrite");
//             }
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_pair() {
        let (sb, ctx1, sc, ctx2) = new_pair_socket();

        let num: usize = 10;
        let mut handles = Vec::with_capacity(2);

        {
            let echo = std::thread::spawn(move || {
                let cp = ChannelPair::new(ctx1, sb).unwrap();
                run_echo(num, &cp)
            });
            handles.push(echo);
        }

        {
            let write = std::thread::spawn(move || {
                let cc = ChannelPair::new(ctx2, sc).unwrap();
                run_write(num, &cc);
            });
            handles.push(write);
        }

        for handle in handles {
            handle.join().expect("Thread panicked!");
        }
    }


    #[test]
    fn test_channel_pair_basic() {
        let ctx = Context::new();
        let server_socket = ctx.socket(zmq::PAIR).unwrap();
        let client_socket = ctx.socket(zmq::PAIR).unwrap();

        server_socket.bind("tcp://127.0.0.1:5555").unwrap();
        client_socket.connect("tcp://127.0.0.1:5555").unwrap();

        let cp = ChannelPair::new(ctx, client_socket).unwrap();

        let msg = vec![b"Hello".to_vec(), b"World".to_vec()];
        cp.tx_chan().send(msg.clone()).unwrap();

        let received_msg = server_socket.recv_multipart(0).unwrap();

        assert!(msg_equal(&msg, &received_msg), "Sent and received messages do not match");
    }
}
