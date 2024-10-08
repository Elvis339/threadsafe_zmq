mod error;

use crossbeam_channel::{
    select, unbounded, Receiver as CrossbeamReceiver, Sender as CrossbeamSender, TryRecvError,
};
use error::ChannelPairError;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use zmq::{Context, PollEvents, Socket};

static UNIQUE_INDEX: AtomicU16 = AtomicU16::new(0);
const IN: usize = 0;
const OUT: usize = 1;

pub type ZmqByteStream = Vec<Vec<u8>>;
pub type Sender = CrossbeamSender<ZmqByteStream>;
pub type Receiver = CrossbeamReceiver<ZmqByteStream>;

pub struct ChannelPair {
    z_sock: Socket,
    z_tx: Vec<Socket>,
    z_control: Vec<Socket>,
    tx_chan: (Sender, Receiver),
    rx_chan: (Sender, Receiver),
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
    pub fn new(context: &Context, socket: Socket) -> Result<Arc<Self>, ChannelPairError> {
        let z_tx = Self::new_pair(context)?;
        let z_control = Self::new_pair(context)?;

        let mut channel_pair = Self {
            z_tx,
            z_control,
            z_sock: socket,
            tx_chan: unbounded::<ZmqByteStream>(),
            rx_chan: unbounded::<ZmqByteStream>(),
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
            self.z_control[OUT].as_poll_item(PollEvents::POLLIN), // z_control for handling control messages
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
                                if let Err(err) = self.rx_writer().send(zmq_byte_stream) {
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

                        // Get and set linger
                        let linger = match self.z_sock.get_linger() {
                            Ok(linger) => linger,
                            Err(e) => {
                                self.on_err(ChannelPairError::Zmq(e));
                                return;
                            }
                        };
                        if let Err(e) = self.z_sock.set_sndtimeo(linger) {
                            self.on_err(ChannelPairError::Zmq(e));
                            return;
                        }

                        // Send any pending message
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

                        items[0].set_events(PollEvents::empty());
                        items[1].set_events(zmq::POLLIN);
                        items[2].set_events(PollEvents::empty());

                        loop {
                            match zmq::poll(&mut items[1..2], 0) {
                                Ok(_) => {
                                    if items[1].is_readable() {
                                        match self.z_tx[OUT].recv_multipart(0) {
                                            Ok(msg) => {
                                                if let Err(e) = self.z_sock.send_multipart(msg, 0) {
                                                    self.on_err(ChannelPairError::Zmq(e));
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                self.on_err(ChannelPairError::Zmq(e));
                                                return;
                                            }
                                        }
                                    } else {
                                        // No more data
                                        break;
                                    }
                                }
                                Err(e) => {
                                    self.on_err(ChannelPairError::Zmq(e));
                                    return;
                                }
                            }
                        }

                        // Now read the tx channel until it is empty
                        loop {
                            match self.tx_reader().try_recv() {
                                Ok(msg) => {
                                    if let Err(e) = self.z_sock.send_multipart(msg, 0) {
                                        self.on_err(ChannelPairError::Zmq(e));
                                        return;
                                    }
                                }
                                Err(TryRecvError::Empty) => {
                                    // No more messages
                                    break;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    self.on_err(ChannelPairError::ChannelError(
                                        "receive channel unexpectedly closed".to_string(),
                                    ));
                                    return;
                                }
                            }
                        }

                        // Exit the function
                        return;
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
        loop {
            select! {
                recv(self.tx_reader()) -> msg => match msg {
                    Ok(msg) => {
                        if let Err(e) = self.z_tx[IN].send_multipart(&msg, 0) {
                            self.on_err(ChannelPairError::Zmq(e));
                            return;
                        }
                    },
                    Err(_) => {
                        self.on_err(ChannelPairError::ChannelError(String::from("tx channel closed unexpectedly")));
                        return;
                    }
                },
                recv(self.rx_control_chan()) -> msg => match msg {
                    Ok(control) => {
                        if control {
                            let _ = self.z_tx[IN].send("", 0);
                        }
                        return;
                    },
                    Err(_) => {
                        self.on_err(ChannelPairError::ChannelError(String::from("tx channel closed unexpectedly")));
                        return;
                    }
                }
            }
        }
    }

    fn rx_writer(&self) -> &Sender {
        &self.rx_chan.0
    }

    pub fn rx(&self) -> &Receiver {
        &self.rx_chan.1
    }

    fn tx_reader(&self) -> &Receiver {
        &self.tx_chan.1
    }

    pub fn tx(&self) -> &Sender {
        &self.tx_chan.0
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
}

fn get_unique_id() -> u16 {
    UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn channel_pair_test() {
        let context = Context::new();

        let sb = context
            .socket(zmq::PAIR)
            .expect("Failed to create PAIR socket sb");
        let sc = context
            .socket(zmq::PAIR)
            .expect("Failed to create PAIR socket sc");

        sb.bind("tcp://127.0.0.1:9737").expect("Failed to bind sb");

        sc.connect("tcp://127.0.0.1:9737")
            .expect("Failed to connect sc");

        let cb = ChannelPair::new(&context, sb).expect("Failed to create ChannelPair cb");
        let cc = ChannelPair::new(&context, sc).expect("Failed to create ChannelPair cc");

        let num = 10;

        let cc_clone = Arc::clone(&cc);
        let t1 = thread::spawn(move || {
            run_echo(num, &cc_clone);
        });

        let cb_clone = Arc::clone(&cb);
        let t2 = thread::spawn(move || {
            run_write(num, cb_clone);
        });

        // Wait for both threads to finish
        t1.join().expect("run_echo thread panicked");
        t2.join().expect("run_write thread panicked");
    }

    fn run_echo(mut num: usize, c: &ChannelPair) {
        let rx = c.rx();
        let tx = c.tx();
        let timeout_duration = std::time::Duration::from_secs(1);
        let mut start_time = std::time::Instant::now();

        loop {
            // Check for timeout
            if start_time.elapsed() > timeout_duration {
                panic!("Timeout in run_echo");
            }

            // Try to receive
            match rx.try_recv() {
                Ok(msg) => {
                    if let Err(err) = tx.try_send(msg) {
                        panic!("Cannot send message back: {:?}", err);
                    }
                    start_time = std::time::Instant::now();
                    num -= 1;
                    if num == 0 {
                        println!("ECHO: done");
                        return;
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // No message to receive at the moment
                }
                Err(_) => {
                    panic!("Cannot read from echo channel");
                }
            }
        }
    }

    fn run_write(num: usize, c: Arc<ChannelPair>) {
        let rx = c.rx();
        let tx = c.tx();
        let src_msg = vec![b"Hello".to_vec(), b"World".to_vec()];

        let tx_count = Arc::new(Mutex::new(0));
        let rx_count = Arc::new(Mutex::new(0));
        let timeout_duration = std::time::Duration::from_secs(1);

        // Spawn a new thread for the producer
        let tx_count_clone = Arc::clone(&tx_count);
        let tx_chan_clone = tx.clone();
        let src_msg_clone = src_msg.clone();
        let producer_handle = thread::spawn(move || {
            loop {
                let mut tx_count = tx_count_clone.lock().unwrap();

                if *tx_count >= num {
                    break;
                }

                // Try to send
                match tx_chan_clone.try_send(src_msg_clone.clone()) {
                    Ok(_) => {
                        *tx_count += 1;
                    }
                    Err(e) => {
                        panic!("Cannot send message: {:?}", e);
                    }
                }

                thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        let mut start_time = std::time::Instant::now();

        loop {
            // Check for timeout
            if start_time.elapsed() > timeout_duration {
                panic!("Timeout in run_write");
            }

            // Try to receive
            match rx.try_recv() {
                Ok(msg) => {
                    let mut rx_count = rx_count.lock().unwrap();
                    *rx_count += 1;

                    if !messages_equal(&msg, &src_msg) {
                        panic!("Messages do not match");
                    }

                    start_time = std::time::Instant::now();

                    if *rx_count >= num {
                        println!("MAIN: done");
                        break;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // No message to receive at the moment
                }
                Err(_) => {
                    panic!("Cannot read from main channel");
                }
            }

            thread::sleep(std::time::Duration::from_millis(1));
        }

        producer_handle.join().expect("Producer thread panicked");
    }

    fn messages_equal(a: &Vec<Vec<u8>>, b: &Vec<Vec<u8>>) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (v1, v2) in a.iter().zip(b.iter()) {
            if v1 != v2 {
                return false;
            }
        }
        true
    }
}
