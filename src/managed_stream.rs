use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::os::fd::AsFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::{io, mem};

use compio::buf::{IntoInner, IoBuf};
use compio::driver::op::{RecvFlags, RecvMsgMulti, RecvMsgMultiResult, ReturnFlags};
use compio::driver::{BufferPool, BufferRef};
use compio::runtime::{Runtime, SubmitMultiStream};
use event_listener::{Event, EventListener};
use futures_util::Stream;

#[derive(Debug)]
pub struct Buffer {
    buf: BufferRef,
    event: Arc<Event>,
}

impl Buffer {
    pub(crate) fn new(buf: BufferRef, event: Arc<Event>) -> Self {
        Buffer { buf, event }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.event.notify(1);
    }
}

impl AsRef<[u8]> for Buffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl AsMut<[u8]> for Buffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut()
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.buf.as_ref()
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut()
    }
}

impl IoBuf for Buffer {
    #[inline]
    fn as_init(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

#[inline]
fn is_pool_exhausted(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ENOBUFS)
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

type RawRecvMsgStream = Pin<Box<dyn Stream<Item = io::Result<RecvMsgMultiResult>>>>;

pub(crate) struct RecvMsgManagedMultiStream<S> {
    fd: S,
    pool: BufferPool,
    returned: Arc<Event>,
    control_len: usize,
    flags: RecvFlags,
    raw: Option<RawRecvMsgStream>,
    wait: Option<EventListener>,
}

impl<S: Clone + AsFd + Unpin + 'static> RecvMsgManagedMultiStream<S> {
    pub(crate) fn new(
        fd: S,
        pool: BufferPool,
        returned: Arc<Event>,
        control_len: usize,
        flags: RecvFlags,
    ) -> Self {
        Self {
            fd,
            pool,
            returned,
            control_len,
            flags,
            raw: None,
            wait: None,
        }
    }

    fn create_raw_stream(&self) -> io::Result<RawRecvMsgStream> {
        let fd = self.fd.clone();
        let pool = self.pool.clone();
        let control_len = self.control_len;
        let flags = self.flags;

        Ok(Box::pin(SubmitMultiStream::new(move || {
            let op = RecvMsgMulti::new(fd.clone(), &pool, control_len, flags)?;

            Ok(Runtime::current()
                .submit_multi(op)
                .into_managed_with(pool.clone(), control_len))
        })))
    }

    fn wait_for_buffer(&mut self) {
        self.raw = None;
        self.wait = Some(self.returned.listen());
    }
}

impl<S: Clone + AsFd + Unpin + 'static> Stream for RecvMsgManagedMultiStream<S> {
    type Item = io::Result<ManagedRecvMsg>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(wait) = self.wait.as_mut() {
                ready!(Pin::new(wait).poll(cx));
                self.wait = None;
                self.raw = None;
            }

            let raw = match self.raw.as_mut() {
                None => match self.create_raw_stream() {
                    Ok(stream) => self.raw.insert(stream),
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                Some(inner) => inner,
            };

            match ready!(raw.as_mut().poll_next(cx)) {
                Some(Ok(msg)) => {
                    return Poll::Ready(Some(Ok(ManagedRecvMsg::new(msg, self.returned.clone()))));
                }

                Some(Err(err)) if is_pool_exhausted(&err) => {
                    self.wait_for_buffer();
                    continue;
                }

                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => self.raw = None,
            }
        }
    }
}

pub(crate) struct ManagedRecvMsg {
    msg: ManuallyDrop<RecvMsgMultiResult>,
    returned: Arc<Event>,
}

impl ManagedRecvMsg {
    fn new(msg: RecvMsgMultiResult, returned: Arc<Event>) -> Self {
        Self {
            msg: ManuallyDrop::new(msg),
            returned,
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.msg.data()
    }

    pub(crate) fn remote(&self) -> Option<SocketAddr> {
        self.msg.addr().and_then(|addr| addr.as_socket())
    }

    pub(crate) fn ancillary(&self) -> &[u8] {
        self.msg.ancillary()
    }

    pub(crate) fn flags(&self) -> ReturnFlags {
        self.msg.flags()
    }

    pub(crate) fn into_buffer(mut self) -> Buffer {
        let msg = unsafe { ManuallyDrop::take(&mut self.msg) };
        let buffer = Buffer::new(msg.into_inner(), self.returned.clone());
        mem::forget(self);

        buffer
    }
}

impl Drop for ManagedRecvMsg {
    fn drop(&mut self) {
        let msg = unsafe { ManuallyDrop::take(&mut self.msg) };
        drop(Buffer::new(msg.into_inner(), self.returned.clone()));
    }
}
