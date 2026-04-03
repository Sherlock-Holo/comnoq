use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use flume::{Receiver, r#async::RecvStream};
use futures_util::Stream;
use noq_proto::{
    ClosePathError, ClosedPath, PathError, PathEvent, PathId, PathStats, PathStatus,
    SetPathStatusError, TransportErrorCode,
};

use crate::{ConnectionInner, sync::shared::Shared};

/// Future produced by [`crate::Connection::open_path`].
#[derive(Debug)]
pub struct OpenPath(OpenPathInner);

#[derive(Debug)]
enum OpenPathInner {
    Ongoing {
        opened: Receiver<Result<(), PathError>>,
        path_id: PathId,
        conn: Shared<ConnectionInner>,
    },
    Rejected {
        err: PathError,
    },
}

impl OpenPath {
    pub(crate) fn new(
        path_id: PathId,
        opened: Receiver<Result<(), PathError>>,
        conn: Shared<ConnectionInner>,
    ) -> Self {
        Self(OpenPathInner::Ongoing {
            opened,
            path_id,
            conn,
        })
    }

    pub(crate) fn rejected(err: PathError) -> Self {
        Self(OpenPathInner::Rejected { err })
    }

    /// Returns the path ID allocated for this path opening attempt.
    pub fn path_id(&self) -> Option<PathId> {
        match self.0 {
            OpenPathInner::Ongoing { path_id, .. } => Some(path_id),
            OpenPathInner::Rejected { .. } => None,
        }
    }
}

impl Future for OpenPath {
    type Output = Result<Path, PathError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            OpenPathInner::Ongoing {
                opened,
                path_id,
                conn,
            } => {
                let mut recv = std::pin::pin!(opened.recv_async());
                match recv.as_mut().poll(cx) {
                    Poll::Ready(Ok(Ok(()))) => {
                        Poll::Ready(Ok(Path::new_unchecked(conn.clone(), *path_id)))
                    }
                    Poll::Ready(Ok(Err(err))) => Poll::Ready(Err(err)),
                    Poll::Ready(Err(_)) => Poll::Ready(Err(PathError::ValidationFailed)),
                    Poll::Pending => Poll::Pending,
                }
            }
            OpenPathInner::Rejected { err } => Poll::Ready(Err(*err)),
        }
    }
}

/// An open path in a multipath-enabled connection.
#[derive(Debug, Clone)]
pub struct Path {
    id: PathId,
    conn: Shared<ConnectionInner>,
}

impl Path {
    pub(crate) fn new_unchecked(conn: Shared<ConnectionInner>, id: PathId) -> Self {
        Self { id, conn }
    }

    /// Returns this path's identifier.
    pub fn id(&self) -> PathId {
        self.id
    }

    /// Returns the current local status for this path.
    pub fn status(&self) -> Result<PathStatus, ClosedPath> {
        self.conn.state().conn.path_status(self.id)
    }

    /// Updates the local status for this path.
    pub fn set_status(&self, status: PathStatus) -> Result<(), SetPathStatusError> {
        let mut state = self.conn.state();
        state.conn.set_path_status(self.id, status)?;
        state.wake();
        Ok(())
    }

    /// Returns statistics for this path.
    pub fn stats(&self) -> Option<PathStats> {
        self.conn.state().path_stats(self.id)
    }

    /// Closes this path locally.
    pub fn close(&self) -> Result<(), ClosePathError> {
        let mut state = self.conn.state();
        let result = state.conn.close_path(
            Instant::now(),
            self.id,
            TransportErrorCode::APPLICATION_ABANDON_PATH.into(),
        );
        state.wake();
        result
    }

    /// Sets the max idle timeout for this path.
    pub fn set_max_idle_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Option<Duration>, ClosedPath> {
        let mut state = self.conn.state();
        let previous = state.conn.set_path_max_idle_timeout(self.id, timeout)?;
        state.wake();
        Ok(previous)
    }

    /// Sets the keep-alive interval for this path.
    pub fn set_keep_alive_interval(
        &self,
        interval: Option<Duration>,
    ) -> Result<Option<Duration>, ClosedPath> {
        let mut state = self.conn.state();
        let previous = state.conn.set_path_keep_alive_interval(self.id, interval)?;
        state.wake();
        Ok(previous)
    }

    /// Tracks changes to the external address reported by the peer for this path.
    pub fn observed_external_addr(&self) -> Result<AddressDiscovery, ClosedPath> {
        let rx = ConnectionInner::subscribe_path_events(&self.conn);
        let state = self.conn.state();
        let initial_value = state.conn.path_observed_address(self.id)?;
        Ok(AddressDiscovery::new(self.id, rx, initial_value))
    }

    /// Returns the peer's UDP address for this path.
    pub fn remote_address(&self) -> Result<SocketAddr, ClosedPath> {
        Ok(self.conn.state().conn.network_path(self.id)?.remote)
    }

    /// Pings the peer over this path.
    pub fn ping(&self) -> Result<(), ClosedPath> {
        let mut state = self.conn.state();
        state.conn.ping_path(self.id)?;
        state.wake();
        Ok(())
    }
}

impl PartialEq for Path {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && Shared::ptr_eq(&self.conn, &other.conn)
    }
}

/// Stream produced by [`Path::observed_external_addr`].
#[derive(Debug)]
pub struct AddressDiscovery {
    path_id: PathId,
    initial_value: Option<SocketAddr>,
    last_value: Option<SocketAddr>,
    events: RecvStream<'static, PathEvent>,
}

impl AddressDiscovery {
    fn new(
        path_id: PathId,
        path_events: Receiver<PathEvent>,
        initial_value: Option<SocketAddr>,
    ) -> Self {
        Self {
            path_id,
            initial_value,
            last_value: initial_value,
            events: path_events.into_stream(),
        }
    }
}

impl Stream for AddressDiscovery {
    type Item = SocketAddr;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(addr) = self.initial_value.take() {
            return Poll::Ready(Some(addr));
        }

        loop {
            match Pin::new(&mut self.events).poll_next(cx) {
                Poll::Ready(Some(PathEvent::ObservedAddr { id, addr })) if id == self.path_id => {
                    if self.last_value != Some(addr) {
                        self.last_value = Some(addr);
                        return Poll::Ready(Some(addr));
                    }
                }
                Poll::Ready(Some(PathEvent::Discarded { id, .. })) if id == self.path_id => {
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(_)) => {}
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
