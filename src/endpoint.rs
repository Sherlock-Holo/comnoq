use std::{
    collections::VecDeque,
    future::poll_fn,
    io, mem,
    mem::ManuallyDrop,
    net::{SocketAddr, SocketAddrV6},
    ops::Deref,
    pin::{Pin, pin},
    ptr,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll, Waker},
    time::Instant,
};

use compio::buf::{BufResult, bytes::Bytes};
#[cfg(rustls)]
use compio::net::ToSocketAddrsAsync;
use compio::net::UdpSocket;
use compio::runtime::JoinHandle;
use compio_log::{Instrument, error};
use flume::{Receiver, Sender, unbounded};
use futures_util::{FutureExt, StreamExt, future::FusedFuture, select, task::AtomicWaker};
use noq_proto::{
    ClientConfig, ConnectError, ConnectionError, ConnectionHandle, DatagramEvent, EndpointConfig,
    EndpointEvent, FourTuple, NetworkChangeHint, ServerConfig, Transmit, VarInt,
};
use rustc_hash::FxHashMap as HashMap;

use crate::{
    Connecting, ConnectionEvent, IO_LOOP_BOUND, Incoming, RECV_TIME_BOUND, RecvMeta,
    SharedSocketState, Socket, SocketEntry, SocketSet, select_socket, spawn_recv_task,
    sync::{atomic::AtomicBool, mutex_blocking::Mutex, shared::Shared},
    work_limiter::WorkLimiter,
};

#[derive(Debug)]
struct EndpointState {
    endpoint: noq_proto::Endpoint,
    worker: Option<JoinHandle<()>>,
    connections: HashMap<ConnectionHandle, Sender<ConnectionEvent>>,
    close: Option<(VarInt, Bytes)>,
    exit_on_idle: bool,
    active_connections: usize,
    incoming: VecDeque<noq_proto::Incoming>,
    incoming_wakers: VecDeque<Waker>,
    all_draining_wakers: VecDeque<Waker>,
    stats: EndpointStats,
}

/// Statistics on [Endpoint] activity
#[non_exhaustive]
#[derive(Debug, Default, Copy, Clone)]
pub struct EndpointStats {
    /// Cumulative number of Quic handshakes accepted by this [Endpoint]
    pub accepted_handshakes: u64,
    /// Cumulative number of Quic handshakes sent from this [Endpoint]
    pub outgoing_handshakes: u64,
    /// Cumulative number of Quic handshakes refused on this [Endpoint]
    pub refused_handshakes: u64,
    /// Cumulative number of Quic handshakes ignored on this [Endpoint]
    pub ignored_handshakes: u64,
}

impl EndpointState {
    fn handle_data(
        &mut self,
        meta: RecvMeta,
        buf: &[u8],
        respond_fn: impl Fn(Vec<u8>, Transmit),
    ) -> usize {
        let now = Instant::now();
        let mut processed = 0;
        for data in buf[..meta.len]
            .chunks(meta.stride.min(meta.len))
            .map(Into::into)
        {
            processed += 1;
            let mut resp_buf = Vec::new();
            match self.endpoint.handle(
                now,
                FourTuple::new(meta.remote, meta.local_ip),
                meta.ecn,
                data,
                &mut resp_buf,
            ) {
                Some(DatagramEvent::NewConnection(incoming)) => {
                    if self.close.is_none() {
                        self.incoming.push_back(incoming);
                    } else {
                        let transmit = self.endpoint.refuse(incoming, &mut resp_buf);
                        respond_fn(resp_buf, transmit);
                    }
                }
                Some(DatagramEvent::ConnectionEvent(ch, event)) => {
                    if let Some(tx) = self.connections.get(&ch) {
                        let _ = tx.send(ConnectionEvent::Proto(event));
                    } else {
                        compio_log::warn!("discarding event for unknown connection {ch:?}");
                    }
                }
                Some(DatagramEvent::Response(transmit)) => respond_fn(resp_buf, transmit),
                None => {}
            }
        }
        processed
    }

    fn handle_event(&mut self, ch: ConnectionHandle, event: EndpointEvent) {
        if event.is_draining() {
            self.active_connections = self.active_connections.saturating_sub(1);
            if self.all_draining() {
                self.all_draining_wakers.drain(..).for_each(Waker::wake);
            }
        }
        if event.is_drained() {
            self.connections.remove(&ch);
        }
        let result = self.endpoint.handle_event(ch, event);
        match (result, self.connections.get(&ch)) {
            (Some(event), Some(tx)) => {
                let _ = tx.send(ConnectionEvent::Proto(event));
            }
            (Some(_), None) => {
                compio_log::warn!("discarding event for connection {ch:?}");
            }
            _ => {}
        }
    }

    fn is_idle(&self) -> bool {
        self.connections.is_empty()
    }

    fn all_draining(&self) -> bool {
        self.active_connections == 0
    }

    fn poll_incoming(&mut self, cx: &mut Context) -> Poll<Option<noq_proto::Incoming>> {
        if self.close.is_none() {
            if let Some(incoming) = self.incoming.pop_front() {
                Poll::Ready(Some(incoming))
            } else {
                self.incoming_wakers.push_back(cx.waker().clone());
                Poll::Pending
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn new_connection(
        &mut self,
        handle: ConnectionHandle,
        conn: noq_proto::Connection,
        sockets: SocketSet,
        events_tx: Sender<(ConnectionHandle, EndpointEvent)>,
    ) -> Connecting {
        let (tx, rx) = unbounded();
        if let Some((error_code, reason)) = &self.close {
            tx.send(ConnectionEvent::Close(*error_code, reason.clone()))
                .unwrap();
        }
        self.active_connections += 1;
        self.connections.insert(handle, tx);
        Connecting::new(handle, conn, sockets, events_tx, rx)
    }
}

impl Drop for EndpointState {
    fn drop(&mut self) {
        for incoming in self.incoming.drain(..) {
            self.endpoint.ignore(incoming);
        }
    }
}

type ChannelPair<T> = (Sender<T>, Receiver<T>);

pub(crate) struct EndpointInner {
    state: Mutex<EndpointState>,
    sockets: SocketSet,
    recv_rx: Receiver<crate::RecvItem>,
    ipv6: AtomicBool,
    events: ChannelPair<(ConnectionHandle, EndpointEvent)>,
    rebind: ChannelPair<Socket>,
    done: AtomicWaker,
}

impl std::fmt::Debug for EndpointInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointInner")
            .field("ipv6", &self.ipv6.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl EndpointInner {
    fn new(
        socket: UdpSocket,
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
    ) -> io::Result<Self> {
        let socket = Socket::new(socket)?;
        let ipv6 = socket.local_addr()?.is_ipv6();
        let allow_mtud = !socket.may_fragment();

        let (recv_tx, recv_rx) = unbounded();
        let endpoint_config = Arc::new(config);
        let max_payload_size = endpoint_config.get_max_udp_payload_size().min(64 * 1024) as usize;

        Ok(Self {
            state: Mutex::new(EndpointState {
                endpoint: noq_proto::Endpoint::new(
                    endpoint_config,
                    server_config.map(Arc::new),
                    allow_mtud,
                ),
                worker: None,
                connections: HashMap::default(),
                close: None,
                exit_on_idle: false,
                active_connections: 0,
                incoming: VecDeque::new(),
                incoming_wakers: VecDeque::new(),
                all_draining_wakers: VecDeque::new(),
                stats: EndpointStats::default(),
            }),
            #[allow(clippy::arc_with_non_send_sync)]
            sockets: Arc::new(std::sync::Mutex::new(SharedSocketState {
                sockets: vec![SocketEntry {
                    socket,
                    local_ip: None,
                }],
                prev_sockets: Vec::new(),
                recv_tx,
                stopped: Arc::new(AtomicBool::new(false)),
                max_payload_size,
            })),
            recv_rx,
            ipv6: AtomicBool::new(ipv6),
            events: unbounded(),
            rebind: unbounded(),
            done: AtomicWaker::new(),
        })
    }

    fn connect(
        &self,
        remote: SocketAddr,
        server_name: &str,
        config: ClientConfig,
    ) -> Result<Connecting, ConnectError> {
        let mut state = self.state.lock();

        if state.worker.is_none() {
            return Err(ConnectError::EndpointStopping);
        }
        let ipv6 = self.ipv6.load(Ordering::Relaxed);
        if remote.is_ipv6() && !ipv6 {
            return Err(ConnectError::InvalidRemoteAddress(remote));
        }
        let remote = if ipv6 {
            SocketAddr::V6(match remote {
                SocketAddr::V4(addr) => {
                    SocketAddrV6::new(addr.ip().to_ipv6_mapped(), addr.port(), 0, 0)
                }
                SocketAddr::V6(addr) => addr,
            })
        } else {
            remote
        };

        let (handle, conn) = state
            .endpoint
            .connect(Instant::now(), config, remote, server_name)?;
        state.stats.outgoing_handshakes += 1;

        Ok(state.new_connection(handle, conn, self.sockets.clone(), self.events.0.clone()))
    }

    fn respond(&self, buf: Vec<u8>, transmit: Transmit) {
        let socket = select_socket(&self.sockets, transmit.src_ip);
        compio::runtime::spawn(async move {
            socket.send(buf, &transmit).await;
        })
        .detach();
    }

    pub(crate) fn accept(
        &self,
        incoming: noq_proto::Incoming,
        server_config: Option<Arc<ServerConfig>>,
    ) -> Result<Connecting, ConnectionError> {
        let mut state = self.state.lock();
        let mut resp_buf = Vec::new();
        let now = Instant::now();
        match state
            .endpoint
            .accept(incoming, now, &mut resp_buf, server_config)
        {
            Ok((handle, conn)) => {
                state.stats.accepted_handshakes += 1;
                Ok(state.new_connection(handle, conn, self.sockets.clone(), self.events.0.clone()))
            }
            Err(err) => {
                if let Some(transmit) = err.response {
                    self.respond(resp_buf, transmit);
                }
                Err(err.cause)
            }
        }
    }

    pub(crate) fn refuse(&self, incoming: noq_proto::Incoming) {
        let mut state = self.state.lock();
        state.stats.refused_handshakes += 1;
        let mut resp_buf = Vec::new();
        let transmit = state.endpoint.refuse(incoming, &mut resp_buf);
        self.respond(resp_buf, transmit);
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn retry(&self, incoming: noq_proto::Incoming) -> Result<(), noq_proto::RetryError> {
        let mut state = self.state.lock();
        let mut resp_buf = Vec::new();
        let transmit = state.endpoint.retry(incoming, &mut resp_buf)?;
        self.respond(resp_buf, transmit);
        Ok(())
    }

    pub(crate) fn ignore(&self, incoming: noq_proto::Incoming) {
        let mut state = self.state.lock();
        state.stats.ignored_handshakes += 1;
        state.endpoint.ignore(incoming);
    }

    fn recv_on(
        socket: Socket,
        buffer: Vec<u8>,
    ) -> impl FusedFuture<Output = (Socket, BufResult<RecvMeta, Vec<u8>>)> {
        async move {
            let result = socket.recv(buffer).await;
            (socket, result)
        }
        .fuse()
    }

    async fn run(&self) -> io::Result<()> {
        let respond_fn = |buf: Vec<u8>, transmit: Transmit| self.respond(buf, transmit);

        let max_payload = self
            .state
            .lock()
            .endpoint
            .config()
            .get_max_udp_payload_size()
            .min(64 * 1024) as usize;

        let primary_socket = {
            let sockets = self.sockets.lock().unwrap();
            for entry in sockets.sockets.iter().skip(1) {
                spawn_recv_task(&self.sockets, &entry.socket, max_payload);
            }
            sockets.sockets[0].socket.clone()
        };

        let recv_buf = Vec::with_capacity(max_payload * primary_socket.max_gro_segments());
        let mut recv_fut = pin!(Self::recv_on(primary_socket, recv_buf));
        let mut recv_stream = self.recv_rx.stream().ready_chunks(IO_LOOP_BOUND);
        let mut event_stream = self.events.1.stream().ready_chunks(IO_LOOP_BOUND);
        let mut rebind_stream = self.rebind.1.stream();
        let mut recv_limiter = WorkLimiter::new(RECV_TIME_BOUND);

        loop {
            let mut state = select! {
                (socket, BufResult(res, recv_buf)) = recv_fut => {
                    let mut state = self.state.lock();
                    match res {
                        Ok(meta) => {
                            state.handle_data(meta, &recv_buf, respond_fn);
                        }
                        Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {}
                        #[cfg(windows)]
                        Err(e) if e.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_PORT_UNREACHABLE as _) => {}
                        Err(e) => break Err(e),
                    }
                    recv_fut.set(Self::recv_on(socket, recv_buf));
                    state
                },
                socket = rebind_stream.select_next_some() => {
                    let recv_buf = Vec::with_capacity(max_payload * socket.max_gro_segments());
                    recv_fut.set(Self::recv_on(socket, recv_buf));
                    self.state.lock()
                },
                items = recv_stream.select_next_some() => {
                    let mut state = self.state.lock();
                    recv_limiter.start_cycle(Instant::now);
                    for (meta, buf) in items {
                        let _ = recv_limiter.allow_work(Instant::now);
                        let processed = state.handle_data(meta, &buf, respond_fn);
                        recv_limiter.record_work(processed);
                    }
                    recv_limiter.finish_cycle(Instant::now);
                    state
                },
                events = event_stream.select_next_some() => {
                    let mut state = self.state.lock();
                    for (ch, event) in events.into_iter().take(IO_LOOP_BOUND) {
                        state.handle_event(ch, event);
                    }
                    state
                },
            };

            if state.is_idle() {
                self.done.wake();
            }
            if state.exit_on_idle && state.is_idle() {
                break Ok(());
            }
            if !state.incoming.is_empty() {
                let n = state.incoming.len().min(state.incoming_wakers.len());
                state.incoming_wakers.drain(..n).for_each(Waker::wake);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EndpointRef(Shared<EndpointInner>);

impl EndpointRef {
    fn into_inner(self) -> Shared<EndpointInner> {
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is not dropped here, and we're consuming Self
        unsafe { ptr::read(&this.0) }
    }

    async fn shutdown(self) -> io::Result<()> {
        self.0
            .sockets
            .lock()
            .unwrap()
            .stopped
            .store(true, Ordering::Relaxed);

        let (worker, idle) = {
            let mut state = self.0.state.lock();
            let idle = state.is_idle();
            if !idle {
                state.exit_on_idle = true;
            }
            (state.worker.take(), idle)
        };
        if let Some(worker) = worker {
            if idle {
                worker.cancel().await;
            } else {
                _ = worker.await;
            }
        }

        let mut this = Some(self.into_inner());
        let inner = poll_fn(move |cx| {
            let s = match Shared::try_unwrap(this.take().unwrap()) {
                Ok(inner) => return Poll::Ready(inner),
                Err(s) => s,
            };

            s.done.register(cx.waker());

            match Shared::try_unwrap(s) {
                Ok(inner) => Poll::Ready(inner),
                Err(s) => {
                    this.replace(s);
                    Poll::Pending
                }
            }
        })
        .await;

        let (sockets, prev_sockets) = {
            let mut sockets = inner.sockets.lock().unwrap();
            (
                sockets.sockets.drain(..).collect::<Vec<_>>(),
                sockets.prev_sockets.drain(..).collect::<Vec<_>>(),
            )
        };
        for entry in sockets {
            if let Err(_e) = entry.socket.close().await {
                error!("failed to close socket: {_e}");
            }
        }
        for socket in prev_sockets {
            if let Err(_e) = socket.close().await {
                error!("failed to close previous socket: {_e}");
            }
        }
        Ok(())
    }
}

impl Drop for EndpointRef {
    fn drop(&mut self) {
        if Shared::strong_count(&self.0) == 2 {
            // There are actually two cases:
            // 1. User is trying to shutdown the socket.
            self.0.done.wake();
            // 2. User dropped the endpoint but the worker is still running.
            self.0.state.lock().exit_on_idle = true;
        }
    }
}

impl Deref for EndpointRef {
    type Target = EndpointInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A QUIC endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    inner: EndpointRef,
    /// The client configuration used by `connect`
    default_client_config: Option<ClientConfig>,
}

impl Endpoint {
    /// Create a QUIC endpoint.
    pub fn new(
        socket: UdpSocket,
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        default_client_config: Option<ClientConfig>,
    ) -> io::Result<Self> {
        let inner = EndpointRef(Shared::new(EndpointInner::new(
            socket,
            config,
            server_config,
        )?));
        let worker = compio::runtime::spawn({
            let inner = inner.clone();
            async move {
                #[allow(unused)]
                if let Err(e) = inner.run().await {
                    error!("I/O error: {}", e);
                }
            }
            .in_current_span()
        });
        inner.state.lock().worker = Some(worker);
        Ok(Self {
            inner,
            default_client_config,
        })
    }

    /// Helper to construct an endpoint for use with outgoing connections only.
    ///
    /// Note that `addr` is the *local* address to bind to, which should usually
    /// be a wildcard address like `0.0.0.0:0` or `[::]:0`, which allow
    /// communication with any reachable IPv4 or IPv6 address respectively
    /// from an OS-assigned port.
    ///
    /// If an IPv6 address is provided, the socket may dual-stack depending on
    /// the platform, so as to allow communication with both IPv4 and IPv6
    /// addresses. As such, calling this method with the address `[::]:0` is a
    /// reasonable default to maximize the ability to connect to other
    /// address.
    ///
    /// IPv4 client is never dual-stack.
    #[cfg(rustls)]
    pub async fn client(addr: impl ToSocketAddrsAsync) -> io::Result<Endpoint> {
        // TODO: try to enable dual-stack on all platforms, notably Windows
        let socket = UdpSocket::bind(addr).await?;
        Self::new(socket, endpoint_config(), None, None)
    }

    /// Helper to construct an endpoint for use with both incoming and outgoing
    /// connections
    ///
    /// Platform defaults for dual-stack sockets vary. For example, any socket
    /// bound to a wildcard IPv6 address on Windows will not by default be
    /// able to communicate with IPv4 addresses. Portable applications
    /// should bind an address that matches the family they wish to
    /// communicate within.
    #[cfg(rustls)]
    pub async fn server(addr: impl ToSocketAddrsAsync, config: ServerConfig) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Self::new(socket, endpoint_config(), Some(config), None)
    }

    /// Returns relevant stats from this Endpoint
    pub fn stats(&self) -> EndpointStats {
        self.inner.state.lock().stats
    }

    /// Get the next incoming connection attempt from a client.
    pub fn accept(&self) -> Accept<'_> {
        Accept { endpoint: self }
    }

    /// Set the client configuration used by `connect`.
    pub fn set_default_client_config(&mut self, config: ClientConfig) {
        self.default_client_config = Some(config);
    }

    /// Connect to a remote endpoint using the default client configuration.
    pub fn connect(
        &self,
        remote: SocketAddr,
        server_name: &str,
    ) -> Result<Connecting, ConnectError> {
        let config = self
            .default_client_config
            .clone()
            .ok_or(ConnectError::NoDefaultClientConfig)?;

        self.connect_with(config, remote, server_name)
    }

    /// Connect to a remote endpoint using a custom configuration.
    pub fn connect_with(
        &self,
        config: ClientConfig,
        remote: SocketAddr,
        server_name: &str,
    ) -> Result<Connecting, ConnectError> {
        self.inner.connect(remote, server_name, config)
    }

    /// Switch to a new UDP socket.
    ///
    /// This updates the endpoint's primary socket and notifies active connections that the local
    /// network path changed. Additional sockets opened through [`crate::Connection::open_path_socket`]
    /// are preserved.
    pub fn rebind(&self, socket: UdpSocket) -> io::Result<()> {
        let socket = Socket::new(socket)?;
        let addr = socket.local_addr()?;
        let old_socket = {
            let mut sockets = self.inner.sockets.lock().unwrap();
            let old = mem::replace(&mut sockets.sockets[0].socket, socket.clone());
            sockets.sockets[0].local_ip = None;
            sockets.prev_sockets.push(old.clone());
            old
        };
        self.inner.ipv6.store(addr.is_ipv6(), Ordering::Relaxed);
        drop(old_socket);
        let _ = self.inner.rebind.0.send(socket);

        let state = self.inner.state.lock();
        for conn in state.connections.values() {
            let _ = conn.send(ConnectionEvent::Rebind);
        }
        Ok(())
    }

    /// Notify connections that the local network address has changed.
    pub fn handle_network_change(&self, hint: Option<Arc<dyn NetworkChangeHint + Send + Sync>>) {
        let state = self.inner.state.lock();
        for conn in state.connections.values() {
            let _ = conn.send(ConnectionEvent::LocalAddressChanged(hint.clone()));
        }
    }

    /// Replace the server configuration, affecting new incoming connections
    /// only.
    ///
    /// Useful for e.g. refreshing TLS certificates without disrupting existing
    /// connections.
    pub fn set_server_config(&self, server_config: Option<ServerConfig>) {
        self.inner
            .state
            .lock()
            .endpoint
            .set_server_config(server_config.map(Arc::new))
    }

    /// Get the local `SocketAddr` the underlying socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.sockets.lock().unwrap().sockets[0]
            .socket
            .local_addr()
    }

    /// Get the number of connections that are currently open.
    pub fn open_connections(&self) -> usize {
        self.inner.state.lock().endpoint.open_connections()
    }

    /// Close all of this endpoint's connections immediately and cease accepting
    /// new connections.
    ///
    /// See [`Connection::close()`] for details.
    ///
    /// [`Connection::close()`]: crate::Connection::close
    pub fn close(&self, error_code: VarInt, reason: &[u8]) {
        let mut state = self.inner.state.lock();
        if state.close.is_some() {
            return;
        }

        let reason = Bytes::copy_from_slice(reason);
        state.close = Some((error_code, reason.clone()));
        for conn in state.connections.values() {
            let _ = conn.send(ConnectionEvent::Close(error_code, reason.clone()));
        }
        state.incoming_wakers.drain(..).for_each(Waker::wake);
        if state.all_draining() {
            state.all_draining_wakers.drain(..).for_each(Waker::wake);
        }
    }

    /// Wait for all connections on the endpoint to enter draining or be drained.
    pub async fn wait_all_draining(&self) {
        poll_fn(|cx| {
            let mut state = self.inner.state.lock();
            if state.all_draining() {
                Poll::Ready(())
            } else {
                state.all_draining_wakers.push_back(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// Wait for all connections on the endpoint to be cleanly shut down.
    pub async fn wait_idle(&self) {
        poll_fn(|cx| {
            let state = self.inner.state.lock();
            if state.is_idle() {
                Poll::Ready(())
            } else {
                self.inner.done.register(cx.waker());
                Poll::Pending
            }
        })
        .await
    }

    /// Gracefully shutdown the endpoint.
    ///
    /// Wait for all connections on the endpoint to be cleanly shut down and
    /// close the underlying socket. This will wait for all clones of the
    /// endpoint, all connections and all streams to be dropped before
    /// closing the socket.
    ///
    /// Waiting for this condition before exiting ensures that a good-faith
    /// effort is made to notify peers of recent connection closes, whereas
    /// exiting immediately could force them to wait out the idle timeout
    /// period.
    ///
    /// Does not proactively close existing connections. Consider calling
    /// [`close()`] if that is desired.
    ///
    /// [`close()`]: Endpoint::close
    pub async fn shutdown(self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}

/// Future produced by [`Endpoint::accept`].
pub struct Accept<'a> {
    endpoint: &'a Endpoint,
}

impl Future for Accept<'_> {
    type Output = Option<Incoming>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.endpoint
            .inner
            .state
            .lock()
            .poll_incoming(cx)
            .map(|incoming| {
                incoming.map(|incoming| Incoming::new(incoming, self.endpoint.inner.clone()))
            })
    }
}

#[cfg(rustls)]
fn endpoint_config() -> EndpointConfig {
    #[cfg(all(feature = "ring", not(feature = "graviola")))]
    {
        EndpointConfig::default()
    }
    #[cfg(feature = "graviola")]
    {
        crate::crypto_graviola::graviola_endpoint_config()
    }
}

#[cfg(feature = "graviola")]
mod seal {
    use noq_proto::{EndpointConfig, ServerConfig};

    pub trait EndpointConfigGraviolaExtSealed {}

    impl EndpointConfigGraviolaExtSealed for EndpointConfig {}

    pub trait ServerConfigGraviolaExtSealed {}

    impl ServerConfigGraviolaExtSealed for ServerConfig {}
}

#[cfg(feature = "graviola")]
/// Extension trait for [`EndpointConfig`] when the `graviola` feature is enabled.
///
/// Provides a graviola-backed alternative to [`EndpointConfig::default()`], which
/// uses the `ring` crate when that feature is active.
pub trait EndpointConfigGraviolaExt: seal::EndpointConfigGraviolaExtSealed {
    /// Create a default [`EndpointConfig`] with a randomized reset key using
    /// graviola HMAC-SHA256.
    fn default_graviola_endpoint_config() -> EndpointConfig;
}

#[cfg(feature = "graviola")]
impl EndpointConfigGraviolaExt for EndpointConfig {
    fn default_graviola_endpoint_config() -> EndpointConfig {
        crate::crypto_graviola::graviola_endpoint_config()
    }
}

#[cfg(feature = "graviola")]
/// Extension trait for [`ServerConfig`] when the `graviola` feature is enabled.
///
/// Provides graviola-backed alternatives to [`ServerConfig::with_crypto()`] and
/// [`ServerConfig::with_single_cert()`], which use the `ring` crate when that
/// feature is active.
pub trait ServerConfigGraviolaExt: seal::ServerConfigGraviolaExtSealed {
    /// Create a [`ServerConfig`] from a QUIC/TLS crypto configuration and a
    /// randomized handshake token key using graviola.
    fn with_graviola_crypto(crypto: Arc<dyn noq_proto::crypto::ServerConfig>) -> ServerConfig;

    /// Create a [`ServerConfig`] from a single certificate chain and private key,
    /// using graviola for handshake token encryption.
    fn with_graviola_single_cert(
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<ServerConfig, rustls::Error>;
}

#[cfg(feature = "graviola")]
impl ServerConfigGraviolaExt for ServerConfig {
    fn with_graviola_crypto(crypto: Arc<dyn noq_proto::crypto::ServerConfig>) -> Self {
        crate::crypto_graviola::graviola_server_with_crypto(crypto)
    }

    fn with_graviola_single_cert(
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, rustls::Error> {
        use noq_proto::crypto::rustls::QuicServerConfig;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};

        let tls_server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)?;

        let quic_server_config = QuicServerConfig::try_from(tls_server_config)
            .map_err(|err| rustls::Error::General(err.to_string()))?;

        Ok(Self::with_graviola_crypto(Arc::new(quic_server_config)))
    }
}
