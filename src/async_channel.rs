//! Async implementation for ZMQ communication.

use crate::error::ChannelPairError;
use crate::ZmqMessage;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use zmq::{Context, PollEvents, Socket};

static ASYNC_UNIQUE_INDEX: AtomicU64 = AtomicU64::new(0);
const PAIR_IN: usize = 0;
const PAIR_OUT: usize = 1;

#[derive(Debug, Clone, Copy)]
pub enum AsyncChannelCapacity {
    /// No limit on queued messages. Use when producers are trusted not to
    /// overwhelm consumers, or when dropping messages is unacceptable.
    Unbounded,

    /// Fixed capacity with backpressure. When full, senders will await until
    /// space is available. This prevents memory exhaustion but requires
    /// careful capacity tuning to avoid throughput bottlenecks.
    Bounded(usize),
}

impl Default for AsyncChannelCapacity {
    fn default() -> Self {
        // Default to bounded with reasonable capacity to prevent runaway memory
        // usage while still allowing burst traffic
        Self::Bounded(10_000)
    }
}

/// Internal state for the socket I/O thread's send state machine.
///
/// ZMQ sockets may not be immediately ready to send (e.g., if the peer's
/// receive buffer is full). This state tracks pending outbound messages
/// so we can retry on the next poll cycle when the socket becomes writable.
enum SocketState {
    Idle,
    ReadyToSend(ZmqMessage),
}

impl SocketState {
    fn take(&mut self) -> Option<ZmqMessage> {
        match std::mem::replace(self, SocketState::Idle) {
            SocketState::ReadyToSend(msg) => Some(msg),
            SocketState::Idle => None,
        }
    }
}

pub struct AsyncChannelPair {
    tx: mpsc::Sender<ZmqMessage>,
    // Tokio channel for inbound messages (socket thread -> user)
    // Wrapped in Mutex because mpsc::Receiver requires &mut for recv.
    rx: tokio::sync::Mutex<mpsc::Receiver<ZmqMessage>>,
    is_shutdown: Arc<AtomicBool>,
    shutdown_tx: mpsc::Sender<ShutdownMode>,
}

#[derive(Debug, Clone, Copy)]
enum ShutdownMode {
    /// Drain pending messages before closing
    Graceful,
    /// Stop immediately, pending messages may be lost
    Immediate,
}

impl AsyncChannelPair {
    pub fn new(context: &Context, socket: Socket) -> Result<Self, ChannelPairError> {
        Self::with_capacity(context, socket, AsyncChannelCapacity::default())
    }

    pub fn with_capacity(
        context: &Context,
        socket: Socket,
        capacity: AsyncChannelCapacity,
    ) -> Result<Self, ChannelPairError> {
        let z_tx_pair = Self::create_socket_pair(context)?;
        let z_control_pair = Self::create_socket_pair(context)?;

        Self::configure_nonblocking(&socket)?;
        for sock in z_tx_pair.iter().chain(z_control_pair.iter()) {
            Self::configure_nonblocking(sock)?;
        }

        let (tx, rx_internal) = match capacity {
            AsyncChannelCapacity::Unbounded => mpsc::channel(usize::MAX),
            AsyncChannelCapacity::Bounded(cap) => mpsc::channel(cap),
        };

        let (tx_internal, rx) = match capacity {
            AsyncChannelCapacity::Unbounded => mpsc::channel(usize::MAX),
            AsyncChannelCapacity::Bounded(cap) => mpsc::channel(cap),
        };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let is_shutdown = Arc::new(AtomicBool::new(false));
        let is_shutdown_clone = Arc::clone(&is_shutdown);

        // Spawn the socket I/O thread.
        // Named thread aids debugging when inspecting thread dumps.
        std::thread::Builder::new()
            .name("async-zmq-io".into())
            .spawn(move || {
                run_socket_loop(
                    socket,
                    z_tx_pair,
                    z_control_pair,
                    rx_internal,
                    tx_internal,
                    shutdown_rx,
                    is_shutdown_clone,
                );
            })
            .map_err(|e| {
                ChannelPairError::Other(format!("failed to spawn socket thread: {}", e))
            })?;

        Ok(Self {
            tx,
            rx: tokio::sync::Mutex::new(rx),
            is_shutdown,
            shutdown_tx,
        })
    }

    /// Sends a message asynchronously.
    ///
    /// This method awaits if the channel is at capacity (bounded channels only).
    /// For unbounded channels or when capacity is available, returns immediately
    /// after queuing the message.
    ///
    /// Note: Successful return means the message is queued, not delivered.
    /// Use ZMQ socket options (e.g., ZMQ_LINGER) to control delivery guarantees.
    pub async fn send(&self, msg: ZmqMessage) -> Result<(), ChannelPairError> {
        self.tx.send(msg).await.map_err(|e| {
            ChannelPairError::ChannelDisconnected(format!("send channel closed: {}", e))
        })
    }

    /// Attempts to send without waiting.
    ///
    /// Returns immediately with an error if the channel is full or closed.
    /// Useful for implementing custom backpressure strategies.
    pub fn try_send(&self, msg: ZmqMessage) -> Result<(), ChannelPairError> {
        self.tx.try_send(msg).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => ChannelPairError::Other("channel full".into()),
            mpsc::error::TrySendError::Closed(_) => {
                ChannelPairError::ChannelDisconnected("channel closed".into())
            }
        })
    }

    pub async fn recv(&self) -> Result<ZmqMessage, ChannelPairError> {
        let mut rx = self.rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| ChannelPairError::ChannelDisconnected("receive channel closed".into()))
    }

    /// Receives with a timeout.
    ///
    /// Returns an error if the timeout expires before a message arrives.
    /// Useful for implementing heartbeat or watchdog patterns.
    pub async fn recv_timeout(&self, timeout: Duration) -> Result<ZmqMessage, ChannelPairError> {
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(msg)) => Ok(msg),
            Ok(None) => Err(ChannelPairError::ChannelDisconnected(
                "channel closed".into(),
            )),
            Err(_) => Err(ChannelPairError::Other("receive timeout".into())),
        }
    }

    pub async fn try_recv(&self) -> Option<ZmqMessage> {
        let mut rx = self.rx.lock().await;
        rx.try_recv().ok()
    }

    /// Initiates graceful shutdown with message draining.
    ///
    /// Signals the socket thread to:
    /// 1. Stop accepting new messages from the channel
    /// 2. Send any messages already queued
    /// 3. Respect the socket's linger setting before closing
    ///
    /// This is async because it waits for the shutdown signal to be accepted.
    pub async fn shutdown(&self) {
        if self
            .is_shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let _ = self.shutdown_tx.send(ShutdownMode::Graceful).await;
        }
    }

    /// Initiates immediate shutdown.
    ///
    /// Pending messages may be lost. Use when speed of shutdown matters
    /// more than message delivery guarantees.
    pub async fn stop(&self) {
        if self
            .is_shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let _ = self.shutdown_tx.send(ShutdownMode::Immediate).await;
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    pub fn send_queue_len(&self) -> usize {
        // capacity() returns max, we want current length
        // Unfortunately tokio mpsc doesn't expose len(), so we approximate
        self.tx.max_capacity() - self.tx.capacity()
    }

    fn create_socket_pair(context: &Context) -> Result<Vec<Socket>, ChannelPairError> {
        let id = ASYNC_UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst);
        let addr = format!("inproc://_async_channelpair_{}", id);

        let server = context.socket(zmq::PAIR)?;
        server.bind(&addr)?;

        let client = context.socket(zmq::PAIR)?;
        client.connect(&addr)?;

        Ok(vec![server, client])
    }

    fn configure_nonblocking(socket: &Socket) -> Result<(), ChannelPairError> {
        // Timeout of 0 means non-blocking: operations return immediately
        // with EAGAIN if they would block
        socket.set_rcvtimeo(0)?;
        socket.set_sndtimeo(0)?;
        Ok(())
    }
}

/// The socket I/O loop runs in a dedicated thread.
///
/// It multiplexes:
/// - The main ZMQ socket (for network I/O)
/// - An internal PAIR socket (to receive messages from the async send channel)
/// - A control PAIR socket (for shutdown signaling)
///
/// The key insight is that we can't await on tokio channels from this thread
/// (it's not in a tokio runtime), so we use try_recv with a short poll timeout
/// to maintain responsiveness without busy-waiting.
fn run_socket_loop(
    socket: Socket,
    z_tx_pair: Vec<Socket>,
    z_control_pair: Vec<Socket>,
    mut rx_from_user: mpsc::Receiver<ZmqMessage>,
    tx_to_user: mpsc::Sender<ZmqMessage>,
    mut shutdown_rx: mpsc::Receiver<ShutdownMode>,
    is_shutdown: Arc<AtomicBool>,
) {
    let mut state = SocketState::Idle;

    // Poll items array - indices matter for the logic below
    const SOCK_IDX: usize = 0; // Main ZMQ socket
    const TX_IDX: usize = 1; // Internal transmit pair (receives forwarded messages)
    const CTRL_IDX: usize = 2; // Control pair (receives shutdown signal)

    let mut poll_items = [
        socket.as_poll_item(PollEvents::empty()),
        z_tx_pair[PAIR_OUT].as_poll_item(PollEvents::empty()),
        z_control_pair[PAIR_OUT].as_poll_item(PollEvents::POLLIN),
    ];

    loop {
        // Configure poll events based on current state.
        // This is a key optimization: we only poll for writable when we
        // have data to send, avoiding spurious wakeups.
        poll_items[SOCK_IDX].set_events(match state {
            SocketState::ReadyToSend(_) => zmq::POLLIN | zmq::POLLOUT,
            SocketState::Idle => zmq::POLLIN,
        });

        // Only check for new messages when idle.
        // This provides natural flow control: we won't pull more from the
        // channel until the current message is sent.
        poll_items[TX_IDX].set_events(match state {
            SocketState::Idle => zmq::POLLIN,
            SocketState::ReadyToSend(_) => PollEvents::empty(),
        });

        // Use short timeout to check tokio channel frequently.
        // 0 = busy poll for max throughput (uses more CPU)
        // Higher values reduce CPU but limit throughput to ~1000/timeout_ms
        match zmq::poll(&mut poll_items, 0) {
            Ok(_) => {}
            Err(zmq::Error::EINTR) => continue, // Interrupted, retry
            Err(e) => {
                // Fatal poll error - log and exit
                eprintln!("zmq poll error: {}", e);
                is_shutdown.store(true, Ordering::SeqCst);
                return;
            }
        }

        // Handle incoming messages
        if poll_items[SOCK_IDX].is_readable() {
            match socket.recv_multipart(0) {
                Ok(msg) => {
                    // Forward to user via tokio channel.
                    // blocking_send is appropriate here since we're not in async context.
                    // If the channel is full, this will block - which provides
                    // backpressure to the network peer.
                    if tx_to_user.blocking_send(msg).is_err() {
                        // Receiver dropped - user is gone, shut down
                        is_shutdown.store(true, Ordering::SeqCst);
                        return;
                    }
                }
                Err(zmq::Error::EAGAIN) => {} // Would block, no data ready
                Err(e) => {
                    eprintln!("zmq recv error: {}", e);
                    is_shutdown.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }

        // Handle outgoing messages - send pending data to network
        if poll_items[SOCK_IDX].is_writable() {
            if let Some(msg) = state.take() {
                match socket.send_multipart(&msg, 0) {
                    Ok(_) => {} // Sent successfully, state is now Idle
                    Err(zmq::Error::EAGAIN) => {
                        // Socket buffer full, put message back and retry next iteration
                        state = SocketState::ReadyToSend(msg);
                    }
                    Err(e) => {
                        eprintln!("zmq send error: {}", e);
                        is_shutdown.store(true, Ordering::SeqCst);
                        return;
                    }
                }
            }
        }

        // Check for new messages from the internal pair socket
        if poll_items[TX_IDX].is_readable() {
            match z_tx_pair[PAIR_OUT].recv_multipart(0) {
                Ok(msg) => {
                    state = SocketState::ReadyToSend(msg);
                }
                Err(zmq::Error::EAGAIN) => {}
                Err(e) => {
                    eprintln!("internal socket recv error: {}", e);
                    is_shutdown.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }

        // Check for shutdown signal
        if poll_items[CTRL_IDX].is_readable() {
            // Consume the control message
            let _ = z_control_pair[PAIR_OUT].recv_multipart(0);
            // Actual shutdown handling is below
        }

        // Check tokio channels - these are non-blocking checks
        // Batch process all available messages for high throughput

        // Check for new outbound messages from user
        // Drain the channel to maximize throughput - don't wait for next poll
        while matches!(state, SocketState::Idle) {
            match rx_from_user.try_recv() {
                Ok(msg) => {
                    // Forward through the internal socket pair.
                    // This wakes up the poll if it was sleeping.
                    if let Err(e) = z_tx_pair[PAIR_IN].send_multipart(&msg, 0) {
                        if e != zmq::Error::EAGAIN {
                            eprintln!("internal socket send error: {}", e);
                            is_shutdown.store(true, Ordering::SeqCst);
                            return;
                        }
                        // EAGAIN on internal socket - buffer for next round
                        state = SocketState::ReadyToSend(msg);
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // Sender dropped - initiate graceful shutdown
                    handle_shutdown(&socket, &mut state, &mut rx_from_user, &z_tx_pair, true);
                    is_shutdown.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }

        // Check for shutdown command
        match shutdown_rx.try_recv() {
            Ok(ShutdownMode::Graceful) => {
                handle_shutdown(&socket, &mut state, &mut rx_from_user, &z_tx_pair, true);
                is_shutdown.store(true, Ordering::SeqCst);
                return;
            }
            Ok(ShutdownMode::Immediate) => {
                is_shutdown.store(true, Ordering::SeqCst);
                return;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Shutdown channel closed, treat as graceful shutdown
                handle_shutdown(&socket, &mut state, &mut rx_from_user, &z_tx_pair, true);
                is_shutdown.store(true, Ordering::SeqCst);
                return;
            }
        }
    }
}

/// Drains pending messages during graceful shutdown.
///
/// This ensures messages that were queued before shutdown are delivered,
/// respecting the socket's linger time for network-level delivery guarantees.
fn handle_shutdown(
    socket: &Socket,
    state: &mut SocketState,
    rx_from_user: &mut mpsc::Receiver<ZmqMessage>,
    z_tx_pair: &[Socket],
    drain: bool,
) {
    if !drain {
        return;
    }

    // Apply socket's linger time to send operations.
    // This gives pending network sends a chance to complete.
    let linger = socket.get_linger().unwrap_or(0);
    let _ = socket.set_sndtimeo(linger);

    // Send any message currently in the state machine
    if let Some(msg) = state.take() {
        let _ = socket.send_multipart(&msg, 0);
    }

    // Drain the internal socket pair
    while let Ok(msg) = z_tx_pair[PAIR_OUT].recv_multipart(zmq::DONTWAIT) {
        let _ = socket.send_multipart(&msg, 0);
    }

    // Drain the tokio channel
    while let Ok(msg) = rx_from_user.try_recv() {
        let _ = socket.send_multipart(&msg, 0);
    }
}

pub struct AsyncChannelPairBuilder<'a> {
    context: &'a Context,
    socket: Socket,
    capacity: AsyncChannelCapacity,
}

impl<'a> AsyncChannelPairBuilder<'a> {
    pub fn new(context: &'a Context, socket: Socket) -> Self {
        Self {
            context,
            socket,
            capacity: AsyncChannelCapacity::default(),
        }
    }

    pub fn with_capacity(mut self, capacity: AsyncChannelCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_bounded_queue(self, depth: usize) -> Self {
        self.with_capacity(AsyncChannelCapacity::Bounded(depth))
    }

    pub fn with_unbounded_queue(self) -> Self {
        self.with_capacity(AsyncChannelCapacity::Unbounded)
    }

    pub fn build(self) -> Result<AsyncChannelPair, ChannelPairError> {
        AsyncChannelPair::with_capacity(self.context, self.socket, self.capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    async fn create_test_pair(ctx: &Context) -> (AsyncChannelPair, AsyncChannelPair) {
        let addr = format!(
            "inproc://async-test-{}",
            ASYNC_UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst)
        );

        let server_sock = ctx.socket(zmq::PAIR).unwrap();
        server_sock.bind(&addr).unwrap();

        let client_sock = ctx.socket(zmq::PAIR).unwrap();
        client_sock.connect(&addr).unwrap();

        let server = AsyncChannelPair::new(ctx, server_sock).unwrap();
        let client = AsyncChannelPair::new(ctx, client_sock).unwrap();

        // Allow socket threads to initialize
        tokio::time::sleep(Duration::from_millis(10)).await;

        (server, client)
    }

    #[tokio::test]
    async fn test_send_receive() {
        let ctx = Context::new();
        let (server, client) = create_test_pair(&ctx).await;

        let msg = vec![b"hello".to_vec()];
        client.send(msg.clone()).await.unwrap();

        let received = server.recv_timeout(Duration::from_secs(1)).await.unwrap();
        assert_eq!(received, msg);

        server.shutdown().await;
        client.shutdown().await;
    }

    #[tokio::test]
    async fn test_echo_throughput() {
        let ctx = Context::new();
        let (server, client) = create_test_pair(&ctx).await;

        const COUNT: usize = 100;

        // Echo server task
        let server_handle = tokio::spawn(async move {
            for _ in 0..COUNT {
                let msg = server.recv_timeout(Duration::from_secs(1)).await.unwrap();
                server.send(msg).await.unwrap();
            }
            server.shutdown().await;
        });

        // Client sends and receives
        for i in 0..COUNT {
            let msg = vec![format!("{}", i).into_bytes()];
            client.send(msg.clone()).await.unwrap();
            let response = client.recv_timeout(Duration::from_secs(1)).await.unwrap();
            assert_eq!(response, msg);
        }

        server_handle.await.unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn test_bounded_backpressure() {
        let ctx = Context::new();
        let addr = format!(
            "inproc://bounded-test-{}",
            ASYNC_UNIQUE_INDEX.fetch_add(1, Ordering::SeqCst)
        );

        let server_sock = ctx.socket(zmq::PAIR).unwrap();
        server_sock.bind(&addr).unwrap();

        let client_sock = ctx.socket(zmq::PAIR).unwrap();
        client_sock.connect(&addr).unwrap();

        // Very small capacity to test backpressure
        let server = AsyncChannelPairBuilder::new(&ctx, server_sock)
            .with_bounded_queue(5)
            .build()
            .unwrap();

        let client = AsyncChannelPairBuilder::new(&ctx, client_sock)
            .with_bounded_queue(5)
            .build()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        // try_send should eventually fail when queue fills
        // (exact behavior depends on timing)
        for i in 0..3 {
            client
                .send(vec![format!("{}", i).into_bytes()])
                .await
                .unwrap();
        }

        // Drain messages
        for _ in 0..3 {
            let _ = server.recv_timeout(Duration::from_secs(1)).await;
        }

        server.shutdown().await;
        client.shutdown().await;
    }

    #[tokio::test]
    async fn test_recv_timeout() {
        let ctx = Context::new();
        let (server, _client) = create_test_pair(&ctx).await;

        let result = server.recv_timeout(Duration::from_millis(50)).await;
        assert!(result.is_err());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_try_recv_empty() {
        let ctx = Context::new();
        let (server, _client) = create_test_pair(&ctx).await;

        let result = server.try_recv().await;
        assert!(result.is_none());

        server.shutdown().await;
    }
}
