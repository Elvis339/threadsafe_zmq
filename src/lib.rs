//! # Thread-Safe ZeroMQ
//!
//! A thread-safe wrapper around ZeroMQ sockets providing safe concurrent access
//! through crossbeam channels, with optional async support via Tokio.
//!
//! ## The Problem
//!
//! ZeroMQ sockets are explicitly NOT thread-safe. The ZMQ guide states:
//! "Do not use or close sockets except in the thread that created them."
//! This is a fundamental design choice in libzmq for performance reasons.
//!
//! However, real-world applications often need to send/receive from multiple
//! threads. This library solves that by isolating all socket operations to
//! dedicated background threads and exposing thread-safe channels to user code.
//!
//! ## Architecture
//!
//! The key insight is that while we can't share ZMQ sockets across threads,
//! we CAN share channels. So we:
//!
//! 1. Dedicate a thread to own and operate the ZMQ socket
//! 2. Use crossbeam channels to communicate between user threads and that socket thread
//! 3. Use internal ZMQ PAIR sockets to signal the socket thread (because zmq_poll
//!    can't wait on channels, only sockets)
//!
//! This gives us O(1) message passing with minimal synchronization overhead.

mod error;

#[cfg(feature = "async")]
mod async_channel;

#[cfg(feature = "async")]
pub use async_channel::{AsyncChannelCapacity, AsyncChannelPair, AsyncChannelPairBuilder};

pub use error::ChannelPairError;

use crossbeam_channel::{
    bounded, select, unbounded, Receiver as CrossbeamReceiver, RecvTimeoutError,
    Sender as CrossbeamSender, TryRecvError, TrySendError,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use zmq::{Context, PollEvents, Socket};

// Global counter for generating unique inproc addresses.
// Each ChannelPair needs unique addresses for its internal socket pairs to avoid
// collisions when multiple ChannelPairs exist in the same process/context.
static UNIQUE_INDEX: AtomicU64 = AtomicU64::new(0);

// Index constants for the socket pair vector.
// PAIR_IN is the "server" side (binds first), PAIR_OUT is the "client" (connects).
// The socket thread reads from PAIR_OUT, the channel thread writes to PAIR_IN.
const PAIR_IN: usize = 0;
const PAIR_OUT: usize = 1;

/// A multipart ZeroMQ message represented as a vector of byte frames.
///
/// ZeroMQ's multipart messages are atomic - all frames are sent/received together.
/// This is essential for protocols like ROUTER/DEALER where the first frame(s)
/// contain routing information that must stay attached to the payload.
pub type ZmqMessage = Vec<Vec<u8>>;

/// Channel capacity configuration.
///
/// The choice between bounded and unbounded has significant implications:
///
/// **Unbounded**: Producers never block, but memory usage is unlimited.
/// Use when: you trust producers not to overwhelm the system, or when
/// dropping messages would be worse than running out of memory.
///
/// **Bounded**: Provides backpressure - fast producers slow down when
/// consumers can't keep up. Use when: you need predictable memory usage,
/// or want to surface performance problems early rather than OOM-ing later.
#[derive(Debug, Clone, Copy, Default)]
pub enum ChannelCapacity {
    #[default]
    Unbounded,
    Bounded(usize),
}

/// Cloneable handle for sending messages.
///
/// Cloning this handle does NOT create a new channel - all clones send to
/// the same underlying channel. This is useful for distributing send capability
/// across multiple producer threads without coordination.
#[derive(Debug, Clone)]
pub struct Sender {
    inner: CrossbeamSender<ZmqMessage>,
}

impl Sender {
    /// Sends a message, blocking if the channel is full (bounded only).
    ///
    /// For unbounded channels, this returns immediately after queuing.
    /// For bounded channels, this blocks until space is available.
    ///
    /// Note: successful return means the message is queued in the channel,
    /// NOT that it has been sent over the network. For delivery guarantees,
    /// configure the underlying ZMQ socket's linger and other options.
    pub fn send(&self, msg: ZmqMessage) -> Result<(), ChannelPairError> {
        self.inner
            .send(msg)
            .map_err(|e| ChannelPairError::ChannelDisconnected(format!("send failed: {}", e)))
    }

    /// Attempts to send without blocking.
    ///
    /// Useful for implementing custom backpressure handling - you can check
    /// if the queue is full and take alternative action (log, drop, etc.)
    /// rather than blocking the caller.
    pub fn try_send(&self, msg: ZmqMessage) -> Result<(), TrySendError<ZmqMessage>> {
        self.inner.try_send(msg)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns current queue depth. Useful for monitoring backpressure.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns capacity, or None for unbounded channels.
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }
}

/// Cloneable handle for receiving messages.
///
/// When multiple receivers exist, they compete for messages (each message
/// goes to exactly one receiver). This enables work-stealing patterns
/// where multiple worker threads pull from a shared queue.
#[derive(Debug, Clone)]
pub struct Receiver {
    inner: CrossbeamReceiver<ZmqMessage>,
}

impl Receiver {
    /// Blocks until a message is available.
    pub fn recv(&self) -> Result<ZmqMessage, ChannelPairError> {
        self.inner
            .recv()
            .map_err(|_| ChannelPairError::ChannelDisconnected("receive channel closed".into()))
    }

    /// Blocks until a message arrives or the timeout expires.
    ///
    /// Useful for implementing periodic housekeeping alongside message processing:
    /// receive with timeout, do maintenance work on timeout, loop.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<ZmqMessage, RecvTimeoutError> {
        self.inner.recv_timeout(timeout)
    }

    /// Returns immediately with the next message or an error if none available.
    pub fn try_recv(&self) -> Result<ZmqMessage, TryRecvError> {
        self.inner.try_recv()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns a blocking iterator over incoming messages.
    pub fn iter(&self) -> impl Iterator<Item = ZmqMessage> + '_ {
        self.inner.iter()
    }

    /// Returns a non-blocking iterator that yields available messages.
    pub fn try_iter(&self) -> impl Iterator<Item = ZmqMessage> + '_ {
        self.inner.try_iter()
    }
}

/// Builder for ChannelPair with fluent configuration API.
pub struct ChannelPairBuilder<'a> {
    context: &'a Context,
    socket: Socket,
    capacity: ChannelCapacity,
}

impl<'a> ChannelPairBuilder<'a> {
    pub fn new(context: &'a Context, socket: Socket) -> Self {
        Self {
            context,
            socket,
            capacity: ChannelCapacity::default(),
        }
    }

    pub fn with_capacity(mut self, capacity: ChannelCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_bounded_queue(self, depth: usize) -> Self {
        self.with_capacity(ChannelCapacity::Bounded(depth))
    }

    pub fn with_unbounded_queue(self) -> Self {
        self.with_capacity(ChannelCapacity::Unbounded)
    }

    pub fn build(self) -> Result<Arc<ChannelPair>, ChannelPairError> {
        ChannelPair::with_capacity(self.context, self.socket, self.capacity)
    }
}

/// State machine for outbound message handling.
///
/// We need this because ZMQ sockets may not be immediately writable (peer's
/// receive buffer full, network congestion, etc.). Rather than busy-loop
/// trying to send, we track pending messages and only poll for POLLOUT
/// when we have something to send.
enum SocketState {
    Idle,
    ReadyToSend(ZmqMessage),
}

impl SocketState {
    fn reset(&mut self) {
        *self = SocketState::Idle;
    }
}

/// Thread-safe wrapper around a ZeroMQ socket.
///
/// ## How It Works
///
/// ChannelPair spawns two background threads:
///
/// 1. **Socket I/O Thread** (`run_sockets`): Owns the ZMQ socket and handles
///    all send/receive operations. Uses zmq_poll to efficiently wait for
///    socket readiness and internal signals.
///
/// 2. **Channel Bridge Thread** (`run_channels`): Bridges between the user-facing
///    crossbeam channel and an internal ZMQ PAIR socket. This is necessary
///    because zmq_poll can't wait on crossbeam channels directly.
///
/// The internal PAIR sockets act as a "wakeup" mechanism - when a message
/// arrives on the crossbeam channel, the bridge thread forwards it to the
/// PAIR socket, which wakes up the zmq_poll in the socket thread.
///
/// ## Memory Safety
///
/// The `unsafe impl Send + Sync` is justified because:
/// - The ZMQ socket is ONLY accessed from the socket I/O thread
/// - User code interacts exclusively through crossbeam channels
/// - Shutdown is coordinated via atomic flags and control channels
///
/// This is a common pattern for encapsulating non-thread-safe resources
/// in a thread-safe wrapper by ensuring single-threaded access internally.
pub struct ChannelPair {
    // The main ZMQ socket. ONLY accessed by the run_sockets thread.
    z_sock: Socket,

    // Internal PAIR socket pair for message forwarding.
    // Channel thread writes to z_tx[PAIR_IN], socket thread reads from z_tx[PAIR_OUT].
    // This converts channel events into pollable socket events.
    z_tx: Vec<Socket>,

    // Internal PAIR socket pair for shutdown signaling.
    // Same pattern as z_tx but dedicated to control messages to avoid
    // priority inversion where shutdown signals get stuck behind data.
    z_control: Vec<Socket>,

    // User-facing send channel. User writes to .0, bridge thread reads from .1
    tx_chan: (CrossbeamSender<ZmqMessage>, CrossbeamReceiver<ZmqMessage>),

    // User-facing receive channel. Socket thread writes to .0, user reads from .1
    rx_chan: (CrossbeamSender<ZmqMessage>, CrossbeamReceiver<ZmqMessage>),

    // Error reporting channel. Background threads report fatal errors here.
    // Unbounded because errors are rare and we don't want error reporting to block.
    error_chan: (
        CrossbeamSender<ChannelPairError>,
        CrossbeamReceiver<ChannelPairError>,
    ),

    // Shutdown control channel. Bool indicates whether to drain (true) or not (false).
    control_chan: (CrossbeamSender<bool>, CrossbeamReceiver<bool>),

    // Atomic flag for fast shutdown checks without channel operations.
    // Use SeqCst ordering because correctness matters more than performance here.
    is_shutdown: AtomicBool,
}

// SAFETY: See struct documentation above for justification.
// TL;DR: ZMQ socket only touched by dedicated thread, everything else is sync.
unsafe impl Send for ChannelPair {}
unsafe impl Sync for ChannelPair {}

impl ChannelPair {
    /// Creates a ChannelPair with unbounded channels.
    ///
    /// The socket should already be bound/connected as appropriate for its type.
    /// This method spawns background threads immediately.
    pub fn new(context: &Context, socket: Socket) -> Result<Arc<Self>, ChannelPairError> {
        Self::with_capacity(context, socket, ChannelCapacity::Unbounded)
    }

    /// Creates a ChannelPair with specified channel capacity.
    pub fn with_capacity(
        context: &Context,
        socket: Socket,
        capacity: ChannelCapacity,
    ) -> Result<Arc<Self>, ChannelPairError> {
        // Create internal socket pairs for signaling.
        // We use ZMQ PAIR because it integrates with zmq_poll and provides
        // exactly-once delivery which simplifies our state management.
        let z_tx = Self::new_pair(context)?;
        let z_control = Self::new_pair(context)?;

        let (tx_chan, rx_chan) = match capacity {
            ChannelCapacity::Unbounded => (unbounded(), unbounded()),
            ChannelCapacity::Bounded(cap) => (bounded(cap), bounded(cap)),
        };

        let mut channel_pair = Self {
            z_tx,
            z_control,
            z_sock: socket,
            tx_chan,
            rx_chan,
            error_chan: unbounded(),
            control_chan: unbounded(),
            is_shutdown: AtomicBool::new(false),
        };

        // Configure all sockets for non-blocking operation.
        // We use zmq_poll for multiplexing, so blocking calls would deadlock.
        Self::configure_socket(&mut channel_pair)?;
        let channel_pair = Arc::new(channel_pair);

        // Spawn socket I/O thread with explicit name for debugging.
        // Named threads make it much easier to diagnose issues in production
        // when looking at thread dumps or profiler output.
        let cp_sockets = Arc::clone(&channel_pair);
        std::thread::Builder::new()
            .name("zmq-socket-io".into())
            .spawn(move || cp_sockets.run_sockets())
            .map_err(|e| {
                ChannelPairError::Other(format!("failed to spawn socket thread: {}", e))
            })?;

        // Spawn channel bridge thread
        let cp_channels = Arc::clone(&channel_pair);
        std::thread::Builder::new()
            .name("zmq-channel-bridge".into())
            .spawn(move || cp_channels.run_channels())
            .map_err(|e| {
                ChannelPairError::Other(format!("failed to spawn channel thread: {}", e))
            })?;

        Ok(channel_pair)
    }

    // === Public API ===

    /// Sends a message through the ZMQ socket.
    ///
    /// The message is queued in the internal channel and will be sent
    /// asynchronously by the background thread. This method blocks only
    /// if the channel is bounded and full.
    pub fn send(&self, msg: ZmqMessage) -> Result<(), ChannelPairError> {
        self.sender().send(msg)
    }

    pub fn try_send(&self, msg: ZmqMessage) -> Result<(), TrySendError<ZmqMessage>> {
        self.sender().try_send(msg)
    }

    /// Receives a message from the ZMQ socket.
    ///
    /// Blocks until a message is available. Messages arrive in the order
    /// they were received from the network (ZMQ guarantees ordering per socket).
    pub fn recv(&self) -> Result<ZmqMessage, ChannelPairError> {
        self.receiver().recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<ZmqMessage, RecvTimeoutError> {
        self.receiver().recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<ZmqMessage, TryRecvError> {
        self.receiver().try_recv()
    }

    /// Returns a cloneable sender handle.
    pub fn sender(&self) -> Sender {
        Sender {
            inner: self.tx_chan.0.clone(),
        }
    }

    /// Returns a cloneable receiver handle.
    pub fn receiver(&self) -> Receiver {
        Receiver {
            inner: self.rx_chan.1.clone(),
        }
    }

    /// Returns the error channel for monitoring background thread failures.
    ///
    /// You should periodically check this, especially in long-running applications.
    /// Errors here indicate the background threads have terminated and the
    /// ChannelPair is no longer functional.
    pub fn error_receiver(&self) -> &CrossbeamReceiver<ChannelPairError> {
        &self.error_chan.1
    }

    /// Gracefully shuts down, draining pending messages first.
    ///
    /// This ensures messages already queued get sent before closing.
    /// The socket's linger setting controls how long we wait for network
    /// delivery of those messages.
    pub fn shutdown(&self) {
        if self
            .is_shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // true = drain pending messages before stopping
            let _ = self.control_chan.0.send(true);
            // Send on control socket to wake zmq_poll
            let _ = self.z_control[PAIR_IN].send("", 0);
        }
    }

    /// Immediately stops without draining. Pending messages may be lost.
    pub fn stop(&self) {
        if self
            .is_shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let _ = self.control_chan.0.send(false);
            let _ = self.z_control[PAIR_IN].send("", 0);
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    // === Deprecated API for backwards compatibility ===

    #[deprecated(since = "2.0.0", note = "use receiver() instead")]
    pub fn rx(&self) -> &CrossbeamReceiver<ZmqMessage> {
        &self.rx_chan.1
    }

    #[deprecated(since = "2.0.0", note = "use sender() instead")]
    pub fn tx(&self) -> &CrossbeamSender<ZmqMessage> {
        &self.tx_chan.0
    }

    #[deprecated(since = "2.0.0", note = "use error_receiver() instead")]
    pub fn rx_err_chan(&self) -> &CrossbeamReceiver<ChannelPairError> {
        &self.error_chan.1
    }

    // === Internal helpers ===

    fn rx_writer(&self) -> &CrossbeamSender<ZmqMessage> {
        &self.rx_chan.0
    }

    fn tx_reader(&self) -> &CrossbeamReceiver<ZmqMessage> {
        &self.tx_chan.1
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

    /// Reports an error and signals shutdown.
    ///
    /// Called by background threads when they encounter fatal errors.
    /// The error is sent to the error channel for user visibility,
    /// and a stop signal is sent to coordinate thread termination.
    fn on_err(&self, error: ChannelPairError) {
        let _ = self.tx_err_chan().send(error);
        let _ = self.tx_control_chan().send(false);
    }

    fn configure_socket(&mut self) -> Result<(), ChannelPairError> {
        // Zero timeout = non-blocking. We rely on zmq_poll for waiting.
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

    /// Creates a connected PAIR socket pair for internal signaling.
    ///
    /// PAIR sockets are ideal here because:
    /// - Bidirectional (though we only use one direction)
    /// - No message routing overhead
    /// - Exactly-once delivery (no drops, no duplicates)
    /// - Work with inproc:// for zero-copy in-process messaging
    fn new_pair(context: &Context) -> Result<Vec<Socket>, ChannelPairError> {
        let id = UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst);
        let addr = format!("inproc://_channelpair_internal-{}", id);

        let server = context.socket(zmq::PAIR)?;
        server.bind(&addr)?;

        let client = context.socket(zmq::PAIR)?;
        client.connect(&addr)?;

        Ok(vec![server, client])
    }

    /// Main socket I/O loop.
    ///
    /// This is the heart of the system. We use zmq_poll to efficiently
    /// multiplex between:
    /// - The main socket (for network I/O)
    /// - The transmit pair socket (to receive messages from user code)
    /// - The control pair socket (to receive shutdown signals)
    ///
    /// The poll-based design means we only wake up when there's work to do,
    /// avoiding CPU waste from busy-polling.
    fn run_sockets(&self) {
        let mut state = SocketState::Idle;

        // Poll item indices - must match the array below
        const SOCK_IDX: usize = 0;
        const TX_IDX: usize = 1;
        const CTRL_IDX: usize = 2;

        let mut items = [
            self.z_sock.as_poll_item(PollEvents::empty()),
            self.z_tx[PAIR_OUT].as_poll_item(PollEvents::empty()),
            self.z_control[PAIR_OUT].as_poll_item(PollEvents::POLLIN),
        ];

        loop {
            // Dynamic poll configuration based on state.
            // Key optimization: only poll for POLLOUT when we have data to send.
            // This prevents spurious wakeups that would waste CPU.
            items[SOCK_IDX].set_events(match state {
                SocketState::ReadyToSend(_) => zmq::POLLOUT,
                SocketState::Idle => zmq::POLLIN,
            });

            // Only accept new messages when idle.
            // This provides natural flow control: we won't dequeue faster
            // than we can send, preventing unbounded internal buffering.
            items[TX_IDX].set_events(match state {
                SocketState::Idle => zmq::POLLIN,
                _ => PollEvents::empty(),
            });

            // -1 timeout = block indefinitely until an event occurs
            match zmq::poll(&mut items, -1) {
                Ok(_) => {
                    // Handle incoming network messages
                    if items[SOCK_IDX].is_readable() {
                        match self.z_sock.recv_multipart(0) {
                            Ok(msg) => {
                                // Forward to user's receive channel.
                                // If this fails, user dropped their receiver - fatal.
                                if let Err(err) = self.rx_writer().send(msg) {
                                    self.on_err(ChannelPairError::ChannelDisconnected(format!(
                                        "failed to forward received message: {}",
                                        err
                                    )));
                                    return;
                                }
                            }
                            Err(e) => {
                                self.on_err(ChannelPairError::Zmq(e));
                                return;
                            }
                        }
                    }

                    // Handle outgoing messages when socket is writable
                    if items[SOCK_IDX].is_writable() {
                        if let SocketState::ReadyToSend(ref msg) = state {
                            match self.z_sock.send_multipart(msg, 0) {
                                Ok(_) => state.reset(),
                                Err(e) => {
                                    self.on_err(ChannelPairError::Zmq(e));
                                    return;
                                }
                            }
                        }
                    }

                    // Handle messages forwarded from the channel bridge
                    if items[TX_IDX].is_readable() {
                        match self.z_tx[PAIR_OUT].recv_multipart(0) {
                            Ok(msg) => state = SocketState::ReadyToSend(msg),
                            Err(e) => {
                                self.on_err(ChannelPairError::Zmq(e));
                                return;
                            }
                        }
                    }

                    // Handle shutdown signal
                    if items[CTRL_IDX].is_readable() {
                        // Consume the signal (we don't care about content)
                        if let Err(e) = self.z_control[PAIR_OUT].recv_multipart(0) {
                            self.on_err(ChannelPairError::Zmq(e));
                            return;
                        }

                        self.handle_shutdown(&mut state);
                        return;
                    }
                }
                Err(e) => {
                    self.on_err(ChannelPairError::Zmq(e));
                    return;
                }
            }
        }
    }

    /// Drains pending messages during graceful shutdown.
    ///
    /// We apply the socket's linger setting to the send timeout so that
    /// network-level sends have a chance to complete. Then we drain both
    /// the internal socket pair and the user channel.
    fn handle_shutdown(&self, state: &mut SocketState) {
        // Apply linger as send timeout to give pending sends a chance
        let linger = self.z_sock.get_linger().unwrap_or(0);
        let _ = self.z_sock.set_sndtimeo(linger);

        // Send any message we were about to send
        if let SocketState::ReadyToSend(ref msg) = state {
            let _ = self.z_sock.send_multipart(msg, 0);
            state.reset();
        }

        // Drain the internal transmit socket pair
        let mut items = [self.z_tx[PAIR_OUT].as_poll_item(zmq::POLLIN)];
        loop {
            match zmq::poll(&mut items, 0) {
                Ok(_) if items[0].is_readable() => {
                    if let Ok(msg) = self.z_tx[PAIR_OUT].recv_multipart(0) {
                        let _ = self.z_sock.send_multipart(msg, 0);
                    }
                }
                _ => break,
            }
        }

        // Drain the user's transmit channel
        while let Ok(msg) = self.tx_reader().try_recv() {
            let _ = self.z_sock.send_multipart(msg, 0);
        }
    }

    /// Channel bridge loop.
    ///
    /// This thread exists because zmq_poll can't directly wait on crossbeam
    /// channels. It sits between the user's channel and an internal ZMQ PAIR
    /// socket, forwarding messages and thus "converting" channel events into
    /// socket events that zmq_poll can handle.
    ///
    /// Uses crossbeam's select! macro to wait on either:
    /// - A message from the user to forward
    /// - A shutdown signal
    fn run_channels(&self) {
        loop {
            select! {
                recv(self.tx_reader()) -> msg => {
                    match msg {
                        Ok(msg) => {
                            // Forward to the internal socket pair.
                            // This wakes up the zmq_poll in run_sockets.
                            if let Err(e) = self.z_tx[PAIR_IN].send_multipart(&msg, 0) {
                                self.on_err(ChannelPairError::Zmq(e));
                                return;
                            }
                        }
                        Err(_) => {
                            // User dropped their sender - exit gracefully
                            return;
                        }
                    }
                }
                recv(self.rx_control_chan()) -> msg => {
                    match msg {
                        Ok(drain) => {
                            if drain {
                                // Send empty message to wake socket thread for draining
                                let _ = self.z_tx[PAIR_IN].send("", 0);
                            }
                            return;
                        }
                        Err(_) => return,
                    }
                }
            }
        }
    }
}

impl Drop for ChannelPair {
    fn drop(&mut self) {
        // Mark as shutdown to prevent further operations.
        // Note: this doesn't drain - for graceful shutdown, call shutdown() explicitly.
        self.is_shutdown.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn create_pair_sockets(ctx: &Context) -> (Socket, Socket) {
        let addr = format!(
            "inproc://test-{}",
            UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst)
        );

        let server = ctx.socket(zmq::PAIR).expect("server socket");
        server.bind(&addr).expect("bind");

        let client = ctx.socket(zmq::PAIR).expect("client socket");
        client.connect(&addr).expect("connect");

        (server, client)
    }

    #[test]
    fn test_send_receive_basic() {
        let ctx = Context::new();
        let (server_sock, client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPair::new(&ctx, server_sock).unwrap();
        let client = ChannelPair::new(&ctx, client_sock).unwrap();

        thread::sleep(Duration::from_millis(10));

        let msg = vec![b"Hello".to_vec(), b"World".to_vec()];
        client.send(msg.clone()).unwrap();

        let received = server.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(received, msg);

        server.shutdown();
        client.shutdown();
    }

    #[test]
    fn test_bounded_channel() {
        let ctx = Context::new();
        let (server_sock, client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPairBuilder::new(&ctx, server_sock)
            .with_bounded_queue(2)
            .build()
            .unwrap();

        let client = ChannelPairBuilder::new(&ctx, client_sock)
            .with_bounded_queue(2)
            .build()
            .unwrap();

        thread::sleep(Duration::from_millis(10));

        client.send(vec![b"1".to_vec()]).unwrap();
        client.send(vec![b"2".to_vec()]).unwrap();

        let msg1 = server.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg1, vec![b"1".to_vec()]);

        let msg2 = server.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg2, vec![b"2".to_vec()]);

        server.shutdown();
        client.shutdown();
    }

    #[test]
    fn test_echo() {
        let ctx = Context::new();
        let (server_sock, client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPair::new(&ctx, server_sock).unwrap();
        let client = ChannelPair::new(&ctx, client_sock).unwrap();

        thread::sleep(Duration::from_millis(10));

        const NUM_MESSAGES: usize = 10;

        let server_clone = Arc::clone(&server);
        let echo_handle = thread::spawn(move || {
            for _ in 0..NUM_MESSAGES {
                let msg = server_clone.recv_timeout(Duration::from_secs(1)).unwrap();
                server_clone.send(msg).unwrap();
            }
        });

        for i in 0..NUM_MESSAGES {
            let msg = vec![format!("message-{}", i).into_bytes()];
            client.send(msg.clone()).unwrap();

            let response = client.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(response, msg);
        }

        echo_handle.join().unwrap();

        server.shutdown();
        client.shutdown();
    }

    #[test]
    fn test_sender_receiver_handles() {
        let ctx = Context::new();
        let (server_sock, client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPair::new(&ctx, server_sock).unwrap();
        let client = ChannelPair::new(&ctx, client_sock).unwrap();

        thread::sleep(Duration::from_millis(10));

        let sender = client.sender();
        let receiver = server.receiver();

        let handle = thread::spawn(move || {
            sender.send(vec![b"from handle".to_vec()]).unwrap();
        });

        handle.join().unwrap();

        let msg = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, vec![b"from handle".to_vec()]);

        server.shutdown();
        client.shutdown();
    }

    #[test]
    fn test_try_recv_empty() {
        let ctx = Context::new();
        let (server_sock, _client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPair::new(&ctx, server_sock).unwrap();

        match server.try_recv() {
            Err(TryRecvError::Empty) => {}
            other => panic!("expected Empty, got {:?}", other),
        }

        server.shutdown();
    }

    #[test]
    fn test_graceful_shutdown() {
        let ctx = Context::new();
        let (server_sock, client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPair::new(&ctx, server_sock).unwrap();
        let client = ChannelPair::new(&ctx, client_sock).unwrap();

        thread::sleep(Duration::from_millis(10));

        client.send(vec![b"test".to_vec()]).unwrap();

        client.shutdown();
        assert!(client.is_shutdown());

        let msg = server.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, vec![b"test".to_vec()]);

        server.shutdown();
    }

    #[test]
    fn test_multipart_message() {
        let ctx = Context::new();
        let (server_sock, client_sock) = create_pair_sockets(&ctx);

        let server = ChannelPair::new(&ctx, server_sock).unwrap();
        let client = ChannelPair::new(&ctx, client_sock).unwrap();

        thread::sleep(Duration::from_millis(10));

        let multipart = vec![
            b"identity".to_vec(),
            b"".to_vec(),
            b"header".to_vec(),
            b"body".to_vec(),
        ];

        client.send(multipart.clone()).unwrap();

        let received = server.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(received.len(), 4);
        assert_eq!(received, multipart);

        server.shutdown();
        client.shutdown();
    }
}
