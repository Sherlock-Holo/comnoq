//! Stream wrappers around connection events.

use std::{
    fmt,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_broadcast::{
    InactiveReceiver, Receiver as AsyncBroadcastReceiver, RecvError, Sender as AsyncBroadcastSender,
};
use event_listener::{Event, EventListener};
use futures_util::Stream;
use noq_proto::{PathEvent, n0_nat_traversal};
use thiserror::Error;

/// The receiver lagged too far behind.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("channel lagged by {0}")]
pub struct Lagged(pub u64);

#[derive(Debug)]
pub(crate) struct Broadcast<T> {
    sender: AsyncBroadcastSender<T>,
    _keepalive: InactiveReceiver<T>,
}

impl<T> Clone for Broadcast<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            _keepalive: self._keepalive.clone(),
        }
    }
}

impl<T: Clone> Broadcast<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        let (mut sender, receiver) = async_broadcast::broadcast(capacity);
        sender.set_overflow(true);
        Self {
            sender,
            _keepalive: receiver.deactivate(),
        }
    }

    pub(crate) fn subscribe(&self) -> BroadcastReceiver<T> {
        BroadcastReceiver {
            inner: self.sender.new_receiver(),
        }
    }

    pub(crate) fn send(&self, event: T) {
        let _ = self.sender.try_broadcast(event);
    }
}

#[derive(Debug)]
pub(crate) struct BroadcastReceiver<T> {
    inner: AsyncBroadcastReceiver<T>,
}

impl<T: Clone> BroadcastReceiver<T> {
    pub(crate) fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, Lagged>>> {
        match Pin::new(&mut self.inner).poll_recv(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(Err(RecvError::Overflowed(count)))) => {
                Poll::Ready(Some(Err(Lagged(count))))
            }
            Poll::Ready(Some(Err(RecvError::Closed))) | Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Watch<T> {
    shared: Arc<WatchShared<T>>,
}

impl<T> Clone for Watch<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Watch<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            shared: Arc::new(WatchShared {
                state: Mutex::new(WatchState {
                    value,
                    version: 0,
                    closed: false,
                }),
                event: Event::new(),
            }),
        }
    }

    pub(crate) fn subscribe(&self) -> WatchReceiver<T> {
        let state = self.shared.state.lock().unwrap();
        WatchReceiver {
            shared: self.shared.clone(),
            next_version: state.version,
            listener: None,
        }
    }

    pub(crate) fn send_if_modified(&self, update: impl FnOnce(&mut T) -> bool) {
        let modified = {
            let mut state = self.shared.state.lock().unwrap();
            if !update(&mut state.value) {
                false
            } else {
                state.version = state.version.wrapping_add(1);
                true
            }
        };
        if modified {
            self.shared.event.notify(usize::MAX);
        }
    }
}

impl<T> Drop for Watch<T> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) != 1 {
            return;
        }
        self.shared.state.lock().unwrap().closed = true;
        self.shared.event.notify(usize::MAX);
    }
}

#[derive(Debug)]
struct WatchShared<T> {
    state: Mutex<WatchState<T>>,
    event: Event,
}

#[derive(Debug)]
struct WatchState<T> {
    value: T,
    version: u64,
    closed: bool,
}

#[derive(Debug)]
pub(crate) struct WatchReceiver<T> {
    shared: Arc<WatchShared<T>>,
    next_version: u64,
    listener: Option<EventListener>,
}

impl<T: Copy> WatchReceiver<T> {
    fn get(&self) -> T {
        self.shared.state.lock().unwrap().value
    }

    fn poll_change(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        loop {
            {
                let state = self.shared.state.lock().unwrap();
                if self.next_version < state.version {
                    self.next_version = state.version;
                    self.listener = None;
                    return Poll::Ready(Some(state.value));
                }
                if state.closed {
                    self.listener = None;
                    return Poll::Ready(None);
                }
            }

            if self.listener.is_none() {
                self.listener = Some(self.shared.event.listen());
                continue;
            }

            let listener = self.listener.as_mut().expect("listener was just checked");
            match Pin::new(listener).poll(cx) {
                Poll::Ready(()) => {
                    self.listener = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// A stream of [`PathEvent`]s for all paths in a connection.
#[derive(Debug)]
pub struct PathEvents {
    inner: BroadcastReceiver<PathEvent>,
}

impl PathEvents {
    pub(crate) fn new(inner: BroadcastReceiver<PathEvent>) -> Self {
        Self { inner }
    }
}

impl Stream for PathEvents {
    type Item = Result<PathEvent, Lagged>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_event(cx)
    }
}

/// A stream of NAT traversal updates for a connection.
#[derive(Debug)]
pub struct NatTraversalUpdates {
    inner: BroadcastReceiver<n0_nat_traversal::Event>,
}

impl NatTraversalUpdates {
    pub(crate) fn new(inner: BroadcastReceiver<n0_nat_traversal::Event>) -> Self {
        Self { inner }
    }
}

impl Stream for NatTraversalUpdates {
    type Item = Result<n0_nat_traversal::Event, Lagged>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_event(cx)
    }
}

/// Watches the external address reported by the peer for this connection.
pub struct ObservedExternalAddr {
    inner: WatchReceiver<Option<SocketAddr>>,
}

impl ObservedExternalAddr {
    pub(crate) fn new(inner: WatchReceiver<Option<SocketAddr>>) -> Self {
        Self { inner }
    }

    /// Returns the most recently observed external address.
    pub fn get(&self) -> Option<SocketAddr> {
        self.inner.get()
    }
}

impl Stream for ObservedExternalAddr {
    type Item = SocketAddr;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.inner.poll_change(cx) {
                Poll::Ready(Some(Some(addr))) => return Poll::Ready(Some(addr)),
                Poll::Ready(Some(None)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl fmt::Debug for ObservedExternalAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedExternalAddr").finish()
    }
}
