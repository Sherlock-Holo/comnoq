use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use flume::Receiver;
use noq_proto::{
    ClosePathError, ClosedPath, PathError, PathId, PathStats, PathStatus, SetPathStatusError,
    TransportErrorCode,
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
        self.conn.state().conn.set_path_status(self.id, status)?;
        Ok(())
    }

    /// Returns statistics for this path.
    pub fn stats(&self) -> Option<PathStats> {
        self.conn.state().path_stats(self.id)
    }

    /// Closes this path locally.
    pub fn close(&self) -> Result<(), ClosePathError> {
        self.conn.state().conn.close_path(
            Instant::now(),
            self.id,
            TransportErrorCode::APPLICATION_ABANDON_PATH.into(),
        )
    }
}
