mod error;
mod utils;

use crossbeam_channel::{
    select, unbounded, Receiver as CrossbeamReceiver, Sender as CrossbeamSender,
};
use error::ChannelPairError;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use zmq::{Context, PollEvents, Socket};

static UNIQUE_INDEX: AtomicU16 = AtomicU16::new(0);
const IN: usize = 0;
const OUT: usize = 1;
const DEFAULT_LINGER_TIME: i32 = 30;

pub type ZmqByteStream = Vec<Vec<u8>>;
pub type Sender = CrossbeamSender<ZmqByteStream>;
pub type Receiver = CrossbeamReceiver<ZmqByteStream>;

pub struct ChannelPair {
    ctx: Context,
    z_sock: Socket,
    z_tx: Vec<Socket>,
    z_control: Vec<Socket>,
    tx_chan: Sender,
    rx_chan: Receiver,
    error_chan: (
        CrossbeamSender<ChannelPairError>,
        CrossbeamReceiver<ChannelPairError>,
    ),
    control_chan: (CrossbeamSender<bool>, CrossbeamReceiver<bool>),
}

enum SocketState {
    Idle,
    ReadyToSend(ZmqByteStream),
}

impl SocketState {
    fn reset(&mut self) {
        *self = SocketState::Idle;
    }
}

unsafe impl Send for ChannelPair {}
unsafe impl Sync for ChannelPair {}

impl ChannelPair {
    pub fn new(ctx: Context, socket: Socket) -> Result<Arc<Self>, ChannelPairError> {
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
            error_chan: unbounded::<ChannelPairError>(),
            control_chan: unbounded::<bool>(),
        };
        Self::configure_socket(&mut channel_pair)?;
        let channel_pair = Arc::new(channel_pair);

        // === run sockets ===
        let channel_pair_clone = Arc::clone(&channel_pair);
        std::thread::spawn(move || channel_pair_clone.run_sockets());

        // === run channels ===
        let channel_pair_clone = Arc::clone(&channel_pair);
        std::thread::spawn(move || channel_pair_clone.run_channels());

        Ok(channel_pair)
    }

    fn run_sockets(&self) {
        let mut state = SocketState::Idle;

        let mut items = [
            self.z_sock.as_poll_item(PollEvents::empty()), // z_sock for reading incoming messages
            self.z_tx[OUT].as_poll_item(PollEvents::empty()), // z_tx[OUT] for receiving messages from `z_tx[IN]`
            self.z_control[OUT].as_poll_item(PollEvents::empty()), // z_control for handling control messages
        ];

        loop {
            // Set events to monitor based on the state
            items[0].set_events(match state {
                SocketState::ReadyToSend(_) => zmq::POLLOUT, // If we have data to send, poll for writable events
                _ => zmq::POLLIN, // If we have no data to send, poll for readable events
            });

            items[1].set_events(match state {
                SocketState::Idle => zmq::POLLIN, // Poll for messages from `z_tx[OUT]` only when idle
                _ => PollEvents::empty(),         // No need to poll if we're in a different state
            });

            match zmq::poll(&mut items, -1) {
                Ok(_) => {
                    // Check if `z_sock` is readable or writable
                    if items[0].is_readable() {
                        match self.z_sock.recv_multipart(0) {
                            Ok(zmq_byte_stream) => {
                                if let Err(err) = self.tx_chan().send(zmq_byte_stream) {
                                    self.on_err(ChannelPairError::ChannelError(format!(
                                        "Failed to send message to channel: {:?}",
                                        err
                                    )));
                                    return;
                                }
                            }
                            Err(recv_err) => {
                                self.on_err(ChannelPairError::Zmq(recv_err));
                                return;
                            }
                        }
                    }

                    if items[0].is_writable() {
                        if let SocketState::ReadyToSend(message) = &state {
                            match self.z_sock.send_multipart(message.clone(), 0) {
                                Ok(_) => {
                                    state.reset();
                                }
                                Err(snd_err) => {
                                    self.on_err(ChannelPairError::Zmq(snd_err));
                                    return;
                                }
                            }
                        }
                    }

                    // Check if there's a message in the transmit socket (`z_tx[OUT]`)
                    if items[1].is_readable() {
                        match self.z_tx[OUT].recv_multipart(0) {
                            Ok(zmq_byte_stream) => {
                                state = SocketState::ReadyToSend(zmq_byte_stream);
                            }
                            Err(z_tx_recv_err) => {
                                self.on_err(ChannelPairError::Zmq(z_tx_recv_err));
                                return;
                            }
                        }
                    }

                    // Handle control messages if there are any
                    if items[2].is_readable() {
                        match self.z_control[OUT].recv_multipart(0) {
                            Ok(_) => {}
                            Err(ctrl_err) => {
                                self.on_err(ChannelPairError::Zmq(ctrl_err));
                                return;
                            }
                        }
                    }
                }
                Err(poll_err) => {
                    self.on_err(ChannelPairError::Zmq(poll_err));
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
                            if let Err(err) = self.z_tx[IN].send_multipart(&msg, 0) {
                                self.on_err(ChannelPairError::Zmq(err));
                                return;
                            }
                        },
                        Err(_) => {
                            self.on_err(ChannelPairError::ChannelError("ZMQ tx channel closed unexpectedly".into()));
                            return;
                        }
                    }
                },
                recv(self.rx_control_chan()) -> control => {
                    match control {
                        Ok(control) => {
                            if control {
                                if let Err(err) = self.z_control[IN].send("", 0) {
                                    self.on_err(ChannelPairError::Zmq(err));
                                }
                            }
                            return;
                        },
                        Err(_) => {
                            self.on_err(ChannelPairError::ChannelError("ZMQ control channel closed unexpectedly".into()));
                            return;
                        }
                    }
                }
            }
        }
    }

    pub fn rx_chan(&self) -> &Receiver {
        &self.rx_chan
    }

    pub fn tx_chan(&self) -> &Sender {
        &self.tx_chan
    }

    pub fn rx_err_chan(&self) -> &CrossbeamReceiver<ChannelPairError> {
        &self.error_chan.1
    }

    fn tx_control_chan(&self) -> &CrossbeamSender<bool> {
        &self.control_chan.0
    }

    fn rx_control_chan(&self) -> &CrossbeamReceiver<bool> {
        &self.control_chan.1
    }

    fn tx_err_chan(&self) -> &CrossbeamSender<ChannelPairError> {
        &self.error_chan.0
    }

    fn on_err(&self, error: ChannelPairError) {
        let _ = self.tx_err_chan().send(error);
        let _ = self.tx_control_chan().send(false);
    }

    fn configure_socket(&mut self) -> Result<(), ChannelPairError> {
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

    fn new_pair(context: &Context) -> Result<Vec<Socket>, ChannelPairError> {
        let addr = format!("inproc://_channelpair_internal-{}", get_unique_id());
        let server_pair = context.socket(zmq::PAIR)?;
        server_pair.bind(&addr)?;

        let client_pair = context.socket(zmq::PAIR)?;
        client_pair.connect(&addr)?;

        Ok(vec![server_pair, client_pair])
    }

    fn disconnect_socket(socket: &Socket) -> Result<(), ChannelPairError> {
        match socket.get_last_endpoint()? {
            Ok(endpoint) => socket
                .disconnect(&endpoint)
                .map_err(|e| ChannelPairError::Zmq(e)),

            Err(bytes) => {
                let endpoint = String::from_utf8_lossy(&bytes).into_owned();
                socket
                    .disconnect(&endpoint)
                    .map_err(|e| ChannelPairError::Zmq(e))
            }
        }
    }
}

fn get_unique_id() -> u16 {
    UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst)
}

// #[test]
//     fn channel_pair() {
//         let (sb, ctx1, sc, ctx2) = new_pair_socket();
//
//         let num: usize = 10;
//
//         // Create a new WaitGroup and add 2 for each spawned thread
//         let wait_group = WaitGroup::new();
//         wait_group.add();
//         wait_group.add();
//
//         // Create and run the echo thread
//         {
//             let wg = wait_group.clone();
//             std::thread::spawn(move || {
//                 let cp = ChannelPair::new(ctx1, sb).unwrap();
//                 run_echo(num, &cp);
//                 wg.done();
//             });
//         }
//
//         // Create and run the write thread
//         {
//             let wg = wait_group.clone();
//             std::thread::spawn(move || {
//                 let cc = ChannelPair::new(ctx2, sc).unwrap();
//                 run_write(num, &cc);
//                 wg.done();
//             });
//         }
//
//         let timeout = Duration::from_secs(5);
//         if !wait_group.wait_timeout(timeout) {
//             panic!("Test timed out after 10 seconds!");
//         }
//
//         println!("All threads completed successfully.");
//     }
//
//     #[test]
//     fn channel_pair_test() {
//         let ctx = Context::new();
//
//         // Create REP socket for the server
//         let server_socket = ctx.socket(zmq::REP).expect("Failed to create REP socket");
//         server_socket
//             .bind("tcp://127.0.0.1:5555")
//             .expect("Failed to bind server socket");
//
//         let client_ctx = Context::new();
//         // Create REQ socket for the client
//         let client_socket = client_ctx
//             .socket(zmq::REQ)
//             .expect("Failed to create REQ socket");
//         client_socket
//             .connect("tcp://127.0.0.1:5555")
//             .expect("Failed to connect client socket");
//
//         // Create a WaitGroup to wait for the server thread to complete
//         let wait_group = WaitGroup::new();
//
//         // Create a shared counter to track remaining messages
//         let num_of_messages = Arc::new(Mutex::new(20)); // Initial message count set to 20
//
//         // Spawn the server thread using a ChannelPair
//         wait_group.add();
//         let wg = wait_group.clone();
//         let num_of_messages_server = Arc::clone(&num_of_messages);
//
//         std::thread::spawn(move || {
//             let channel_pair = ChannelPair::new(ctx, server_socket).unwrap();
//
//             while *num_of_messages_server.lock().unwrap() > 0 {
//                 // Wait for a message from the client
//                 match channel_pair.rx_chan().recv() {
//                     Ok(message) => {
//                         let message_clone = message.clone();
//
//                         // Spawn a new thread for each message to process it
//                         let cp_clone = Arc::clone(&channel_pair);
//                         let num_of_messages_server = Arc::clone(&num_of_messages_server);
//                         std::thread::spawn(move || {
//                             // Simulate message processing
//                             std::thread::sleep(Duration::from_millis(5)); // Simulate processing time
//
//                             // Send the same message back as the response
//                             cp_clone
//                                 .tx_chan()
//                                 .send(message_clone)
//                                 .expect("Failed to send response");
//
//                             // Decrement the shared counter
//                             let mut num = num_of_messages_server.lock().unwrap();
//                             *num -= 1;
//
//                             // Exit the thread once done
//                             if *num == 0 {
//                                 println!("Server: All messages processed.");
//                             }
//                         });
//                     }
//                     Err(err) => {
//                         eprintln!("Server: Failed to receive message: {:?}", err);
//                         break;
//                     }
//                 }
//             }
//
//             wg.done(); // Mark the server thread as done
//         });
//
//         // Client side logic
//         let mut num_of_messages_client = 20;
//
//         while num_of_messages_client > 0 {
//             // Send a message to the server
//             let msg = vec![b"Hello".to_vec()];
//             client_socket
//                 .send_multipart(&msg, 0)
//                 .expect("Failed to send message from client");
//
//             // Wait for a response from the server
//             let response = client_socket
//                 .recv_multipart(0)
//                 .expect("Failed to receive message from server");
//             println!("Client: Received response: {:?}", response);
//
//             // Decrement the number of remaining messages
//             num_of_messages_client -= 1;
//         }
//
//         // Wait for the server to finish processing all messages, with a timeout of 10 seconds
//         if !wait_group.wait_timeout(Duration::from_secs(5)) {
//             panic!("Test timed out after 10 seconds!");
//         }
//
//         println!("All messages processed successfully.");
//     }
// }
