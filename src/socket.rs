//! Simple wrapper around UDP socket with advanced features useful for QUIC,
//! ported from [`quinn-udp`]
//!
//! Differences from [`quinn-udp`]:
//! - [quinn-rs/quinn#1516] is not implemented
//! - batch receiving is implemented with compio's multishot `RecvMsgMulti`
//!
//! [`quinn-udp`]: https://docs.rs/quinn-udp
//! [quinn-rs/quinn#1516]: https://github.com/quinn-rs/quinn/pull/1516

use std::{
    future::Future,
    io,
    mem::MaybeUninit,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    os::fd::AsFd,
    pin::Pin,
    sync::{Arc, Mutex, atomic::Ordering},
    task::{Context, Poll, ready},
};

use compio::buf::{BufResult, IntoInner, IoBuf, IoBufMut, SetLen};
use compio::driver::{
    BoxAllocator, BufferPool, ToSharedFd,
    op::{RecvFlags, ReturnFlags},
};
use compio::io::ancillary::{AncillaryBuilder, AncillaryData, AncillaryIter};
use compio::net::UdpSocket;
use compio::runtime;
use event_listener::Event;
use flume::Sender;
use futures_util::{Stream, StreamExt};
use noq_proto::{EcnCodepoint, Transmit};
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock;

use crate::managed_stream::{Buffer, ManagedRecvMsg, RecvMsgManagedMultiStream};
use crate::sync::atomic::AtomicBool;

const CMSG_LEN: usize = 128;
const RECV_POOL_SIZE: u16 = 64;

#[repr(align(16))]
struct AlignedControlBytes([u8; CMSG_LEN]);

struct ControlBuf {
    buf: AlignedControlBytes,
    len: usize,
}

impl ControlBuf {
    #[inline]
    fn new() -> Self {
        Self {
            buf: AlignedControlBytes([0; CMSG_LEN]),
            len: 0,
        }
    }

    #[inline]
    fn as_uninit_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: `u8` and `MaybeUninit<u8>` have identical layout.
        unsafe { std::slice::from_raw_parts_mut(self.buf.0.as_mut_ptr().cast(), CMSG_LEN) }
    }
}

impl IoBuf for ControlBuf {
    #[inline]
    fn as_init(&self) -> &[u8] {
        &self.buf.0[..self.len]
    }
}

impl SetLen for ControlBuf {
    #[inline]
    unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= CMSG_LEN);
        self.len = len;
    }
}

impl IoBufMut for ControlBuf {
    #[inline]
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        self.as_uninit_slice()
    }
}

/// Metadata for a single buffer filled with bytes received from the network
///
/// This associated buffer can contain one or more datagrams, see [`stride`].
///
/// [`stride`]: RecvMeta::stride
#[derive(Debug)]
pub(crate) struct RecvMeta {
    /// The source address of the datagram(s) contained in the buffer
    pub remote: SocketAddr,
    /// The number of bytes the associated buffer has
    pub len: usize,
    /// The size of a single datagram in the associated buffer
    ///
    /// When GRO (Generic Receive Offload) is used this indicates the size of a
    /// single datagram inside the buffer. If the buffer is larger, that is
    /// if [`len`] is greater then this value, then the individual datagrams
    /// contained have their boundaries at `stride` increments from the
    /// start. The last datagram could be smaller than `stride`.
    ///
    /// [`len`]: RecvMeta::len
    pub stride: usize,
    /// The Explicit Congestion Notification bits for the datagram(s) in the
    /// buffer
    pub ecn: Option<EcnCodepoint>,
    /// The destination IP address which was encoded in this datagram
    ///
    /// Populated on platforms: Windows, Linux, Android, FreeBSD, OpenBSD,
    /// NetBSD, macOS, and iOS.
    pub local_ip: Option<IpAddr>,
}

impl RecvMeta {
    fn from_parts(remote: SocketAddr, len: usize, ancillary: &[u8]) -> Self {
        let (ecn_bits, local_ip, stride) = Self::ancillary_fields(ancillary, len);
        Self {
            remote,
            len,
            stride,
            ecn: EcnCodepoint::from_bits(ecn_bits),
            local_ip,
        }
    }

    fn ancillary_fields(control: &[u8], len: usize) -> (u8, Option<IpAddr>, usize) {
        let mut ecn_bits = 0u8;
        let mut local_ip = None;
        #[allow(unused_mut)]
        let mut stride = len;

        if !control.is_empty() {
            // SAFETY: `control` is initialized by recvmsg and contains valid control messages
            // returned by the OS.
            for cmsg in unsafe { AncillaryIter::new(control) } {
                #[cfg(windows)]
                const UDP_COALESCED_INFO: i32 = WinSock::UDP_COALESCED_INFO as i32;

                match (cmsg.level(), cmsg.ty()) {
                    // ECN
                    #[cfg(unix)]
                    (libc::IPPROTO_IP, libc::IP_TOS) => {
                        ecn_bits = cmsg.data::<u8>().expect("valid IP_TOS cmsg")
                    }
                    #[cfg(all(unix, not(any(non_freebsd, solarish))))]
                    (libc::IPPROTO_IP, libc::IP_RECVTOS) => {
                        ecn_bits = cmsg.data::<u8>().expect("valid IP_RECVTOS cmsg")
                    }
                    #[cfg(unix)]
                    (libc::IPPROTO_IPV6, libc::IPV6_TCLASS) => {
                        // NOTE: It's OK to use `c_int` instead of `u8` on Apple systems
                        ecn_bits = cmsg.data::<libc::c_int>().expect("valid IPV6_TCLASS cmsg") as u8
                    }
                    #[cfg(windows)]
                    (WinSock::IPPROTO_IP, WinSock::IP_ECN)
                    | (WinSock::IPPROTO_IPV6, WinSock::IPV6_ECN) => {
                        ecn_bits = cmsg.data::<i32>().expect("valid IP_ECN cmsg") as u8
                    }

                    // pktinfo / destination address
                    #[cfg(linux_all)]
                    (libc::IPPROTO_IP, libc::IP_PKTINFO) => {
                        let pktinfo = cmsg
                            .data::<libc::in_pktinfo>()
                            .expect("valid IP_PKTINFO cmsg");
                        local_ip = Some(IpAddr::from(pktinfo.ipi_addr.s_addr.to_ne_bytes()));
                    }
                    #[cfg(any(bsd, solarish, apple))]
                    (libc::IPPROTO_IP, libc::IP_RECVDSTADDR) => {
                        let in_addr = cmsg
                            .data::<libc::in_addr>()
                            .expect("valid IP_RECVDSTADDR cmsg");
                        local_ip = Some(IpAddr::from(in_addr.s_addr.to_ne_bytes()));
                    }
                    #[cfg(windows)]
                    (WinSock::IPPROTO_IP, WinSock::IP_PKTINFO) => {
                        let pktinfo = cmsg
                            .data::<WinSock::IN_PKTINFO>()
                            .expect("valid IP_PKTINFO cmsg");
                        local_ip = Some(IpAddr::from(
                            // SAFETY: S_addr is a valid representation of the union for IPv4
                            // addresses
                            unsafe { pktinfo.ipi_addr.S_un.S_addr }.to_ne_bytes(),
                        ));
                    }
                    #[cfg(unix)]
                    (libc::IPPROTO_IPV6, libc::IPV6_PKTINFO) => {
                        let pktinfo = cmsg
                            .data::<libc::in6_pktinfo>()
                            .expect("valid IPV6_PKTINFO cmsg");
                        local_ip = Some(IpAddr::from(pktinfo.ipi6_addr.s6_addr));
                    }
                    #[cfg(windows)]
                    (WinSock::IPPROTO_IPV6, WinSock::IPV6_PKTINFO) => {
                        let pktinfo = cmsg
                            .data::<WinSock::IN6_PKTINFO>()
                            .expect("valid IPV6_PKTINFO cmsg");
                        // SAFETY: Byte is a valid representation of the union for IPv6 addresses
                        local_ip = Some(IpAddr::from(unsafe { pktinfo.ipi6_addr.u.Byte }));
                    }

                    // GRO
                    #[cfg(linux_all)]
                    (libc::SOL_UDP, libc::UDP_GRO) => {
                        stride = cmsg.data::<libc::c_int>().expect("valid UDP_GRO cmsg") as usize
                    }
                    #[cfg(windows)]
                    (WinSock::IPPROTO_UDP, UDP_COALESCED_INFO) => {
                        stride = cmsg.data::<u32>().expect("valid UDP_COALESCED_INFO cmsg") as usize
                    }

                    _ => {}
                }
            }
        }

        (ecn_bits, local_ip, stride)
    }
}

fn push_cmsg<T: AncillaryData>(
    builder: &mut AncillaryBuilder<'_, impl IoBufMut + ?Sized>,
    level: i32,
    ty: i32,
    value: T,
) -> io::Result<()> {
    builder
        .push(level, ty, &value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cmsg_len is too small"))
}

#[cfg(linux_all)]
#[inline]
fn max_gso_segments(socket: &UdpSocket) -> io::Result<usize> {
    unsafe {
        socket.get_socket_option::<libc::c_int>(libc::SOL_UDP, libc::UDP_SEGMENT)?;
    }
    Ok(32)
}
#[cfg(windows)]
#[inline]
fn max_gso_segments(socket: &UdpSocket) -> io::Result<usize> {
    unsafe {
        socket.get_socket_option::<i32>(WinSock::IPPROTO_UDP, WinSock::UDP_SEND_MSG_SIZE)?;
    }
    Ok(512)
}
#[cfg(not(any(linux_all, windows)))]
#[inline]
fn max_gso_segments(_socket: &UdpSocket) -> io::Result<usize> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

#[inline]
fn error_is_unsupported(e: &io::Error) -> bool {
    if matches!(
        e.kind(),
        io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
    ) {
        return true;
    }
    let Some(raw) = e.raw_os_error() else {
        return false;
    };
    #[cfg(unix)]
    {
        raw == libc::ENOPROTOOPT
    }
    #[cfg(windows)]
    {
        raw == WinSock::WSAENOPROTOOPT
    }
}

macro_rules! set_socket_option {
    ($socket:expr, $level:expr, $name:expr, $value:expr $(,)?) => {
        match unsafe { $socket.set_socket_option($level, $name, $value) } {
            Ok(()) => true,
            Err(e) if error_is_unsupported(&e) => false,
            Err(e) => {
                compio_log::warn!(
                    level = stringify!($level),
                    name = stringify!($name),
                    "failed to set socket option: {}",
                    e
                );
                return Err(e);
            }
        }
    };
}

#[derive(Debug)]
pub(crate) struct Socket {
    inner: UdpSocket,
    max_gro_segments: usize,
    max_gso_segments: NonZeroUsize,
    may_fragment: bool,
    has_gso_error: AtomicBool,
    #[cfg(freebsd)]
    encode_src_ip_v4: bool,
}

impl Socket {
    pub fn new(socket: UdpSocket) -> io::Result<Self> {
        let is_ipv6 = socket.local_addr()?.is_ipv6();
        #[cfg(unix)]
        let only_v6 = unsafe {
            is_ipv6
                && socket.get_socket_option::<libc::c_int>(libc::IPPROTO_IPV6, libc::IPV6_V6ONLY)?
                    != 0
        };
        #[cfg(windows)]
        let only_v6 = unsafe {
            is_ipv6
                && socket.get_socket_option::<u8>(WinSock::IPPROTO_IPV6, WinSock::IPV6_V6ONLY)? != 0
        };
        let is_ipv4 = socket.local_addr()?.is_ipv4() || !only_v6;

        // ECN
        if is_ipv4 {
            #[cfg(all(unix, not(any(non_freebsd, solarish))))]
            set_socket_option!(socket, libc::IPPROTO_IP, libc::IP_RECVTOS, &1);
            #[cfg(windows)]
            set_socket_option!(socket, WinSock::IPPROTO_IP, WinSock::IP_RECVECN, &1);
        }
        if is_ipv6 {
            #[cfg(unix)]
            set_socket_option!(socket, libc::IPPROTO_IPV6, libc::IPV6_RECVTCLASS, &1);
            #[cfg(windows)]
            set_socket_option!(socket, WinSock::IPPROTO_IPV6, WinSock::IPV6_RECVECN, &1);
        }

        // pktinfo / destination address
        if is_ipv4 {
            #[cfg(linux_all)]
            set_socket_option!(socket, libc::IPPROTO_IP, libc::IP_PKTINFO, &1);
            #[cfg(any(bsd, solarish, apple))]
            set_socket_option!(socket, libc::IPPROTO_IP, libc::IP_RECVDSTADDR, &1);
            #[cfg(windows)]
            set_socket_option!(socket, WinSock::IPPROTO_IP, WinSock::IP_PKTINFO, &1);
        }
        if is_ipv6 {
            #[cfg(unix)]
            set_socket_option!(socket, libc::IPPROTO_IPV6, libc::IPV6_RECVPKTINFO, &1);
            #[cfg(windows)]
            set_socket_option!(socket, WinSock::IPPROTO_IPV6, WinSock::IPV6_PKTINFO, &1);
        }

        // disable fragmentation
        #[allow(unused_mut)]
        let mut may_fragment = false;
        if is_ipv4 {
            #[cfg(linux_all)]
            {
                may_fragment |= set_socket_option!(
                    socket,
                    libc::IPPROTO_IP,
                    libc::IP_MTU_DISCOVER,
                    &libc::IP_PMTUDISC_PROBE,
                );
            }
            #[cfg(any(aix, freebsd, apple))]
            {
                may_fragment |= set_socket_option!(socket, libc::IPPROTO_IP, libc::IP_DONTFRAG, &1);
            }
            #[cfg(windows)]
            {
                may_fragment |=
                    set_socket_option!(socket, WinSock::IPPROTO_IP, WinSock::IP_DONTFRAGMENT, &1);
            }
        }
        if is_ipv6 {
            #[cfg(linux_all)]
            {
                may_fragment |= set_socket_option!(
                    socket,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_MTU_DISCOVER,
                    &libc::IPV6_PMTUDISC_PROBE,
                );
            }
            #[cfg(unix)]
            {
                may_fragment |=
                    set_socket_option!(socket, libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG, &1);
            }
            #[cfg(windows)]
            {
                may_fragment |=
                    set_socket_option!(socket, WinSock::IPPROTO_IPV6, WinSock::IPV6_DONTFRAG, &1);
            }
        }

        // GRO
        #[allow(unused_mut)]
        let mut max_gro_segments = 1;
        #[cfg(linux_all)]
        if set_socket_option!(socket, libc::SOL_UDP, libc::UDP_GRO, &1) {
            max_gro_segments = 64;
        }
        #[cfg(all(windows, feature = "windows-gro"))]
        if set_socket_option!(
            socket,
            WinSock::IPPROTO_UDP,
            WinSock::UDP_RECV_MAX_COALESCED_SIZE,
            &(u16::MAX as u32),
        ) {
            max_gro_segments = 64;
        }

        // GSO
        let max_gso_segments = NonZeroUsize::new(max_gso_segments(&socket).unwrap_or(1))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "max_gso_segments is 0"))?;

        #[cfg(freebsd)]
        let encode_src_ip_v4 =
            socket.local_addr().unwrap().ip() == IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);

        Ok(Self {
            inner: socket,
            max_gro_segments,
            max_gso_segments,
            may_fragment,
            has_gso_error: AtomicBool::new(false),
            #[cfg(freebsd)]
            encode_src_ip_v4,
        })
    }

    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    #[inline]
    pub fn may_fragment(&self) -> bool {
        self.may_fragment
    }

    #[inline]
    pub fn max_gro_segments(&self) -> usize {
        self.max_gro_segments
    }

    #[inline]
    pub fn max_gso_segments(&self) -> NonZeroUsize {
        if self.has_gso_error.load(Ordering::Relaxed) {
            NonZeroUsize::new(1).unwrap()
        } else {
            self.max_gso_segments
        }
    }

    pub fn recv_multi(
        &self,
        pool: &EndpointRecvPool,
    ) -> RecvMultiStream<impl Clone + AsFd + Unpin + 'static> {
        RecvMultiStream::new(RecvMsgManagedMultiStream::new(
            self.inner.to_shared_fd(),
            pool.pool.clone(),
            pool.returned.clone(),
            CMSG_LEN,
            RecvFlags::empty(),
        ))
    }

    fn construct_control_message(&self, transmit: &Transmit) -> io::Result<ControlBuf> {
        let is_ipv4 = transmit.destination.ip().to_canonical().is_ipv4();
        let ecn = transmit.ecn.map_or(0, |x| x as u8);

        let mut control_buf = ControlBuf::new();
        let mut builder = AncillaryBuilder::new(&mut control_buf);

        // ECN
        if is_ipv4 {
            #[cfg(all(unix, not(any(freebsd, netbsd))))]
            push_cmsg(
                &mut builder,
                libc::IPPROTO_IP,
                libc::IP_TOS,
                ecn as libc::c_int,
            )?;
            #[cfg(freebsd)]
            push_cmsg(
                &mut builder,
                libc::IPPROTO_IP,
                libc::IP_TOS,
                ecn as libc::c_uchar,
            )?;
            #[cfg(windows)]
            push_cmsg(
                &mut builder,
                WinSock::IPPROTO_IP,
                WinSock::IP_ECN,
                ecn as i32,
            )?;
        } else {
            #[cfg(unix)]
            push_cmsg(
                &mut builder,
                libc::IPPROTO_IPV6,
                libc::IPV6_TCLASS,
                ecn as libc::c_int,
            )?;
            #[cfg(windows)]
            push_cmsg(
                &mut builder,
                WinSock::IPPROTO_IPV6,
                WinSock::IPV6_ECN,
                ecn as i32,
            )?;
        }

        // pktinfo / destination address
        match transmit.src_ip {
            Some(IpAddr::V4(ip)) => {
                let addr = u32::from_ne_bytes(ip.octets());
                #[cfg(linux_all)]
                {
                    let pktinfo = libc::in_pktinfo {
                        ipi_ifindex: 0,
                        ipi_spec_dst: libc::in_addr { s_addr: addr },
                        ipi_addr: libc::in_addr { s_addr: 0 },
                    };
                    push_cmsg(&mut builder, libc::IPPROTO_IP, libc::IP_PKTINFO, pktinfo)?;
                }
                #[cfg(any(bsd, solarish, apple))]
                {
                    #[cfg(freebsd)]
                    let encode_src_ip_v4 = self.encode_src_ip_v4;
                    #[cfg(any(non_freebsd, solarish, apple))]
                    let encode_src_ip_v4 = true;

                    if encode_src_ip_v4 {
                        let addr = libc::in_addr { s_addr: addr };
                        push_cmsg(&mut builder, libc::IPPROTO_IP, libc::IP_RECVDSTADDR, addr)?;
                    }
                }
                #[cfg(windows)]
                {
                    let pktinfo = WinSock::IN_PKTINFO {
                        ipi_addr: WinSock::IN_ADDR {
                            S_un: WinSock::IN_ADDR_0 { S_addr: addr },
                        },
                        ipi_ifindex: 0,
                    };
                    push_cmsg(
                        &mut builder,
                        WinSock::IPPROTO_IP,
                        WinSock::IP_PKTINFO,
                        pktinfo,
                    )?;
                }
            }
            Some(IpAddr::V6(ip)) => {
                #[cfg(unix)]
                {
                    let pktinfo = libc::in6_pktinfo {
                        ipi6_ifindex: 0,
                        ipi6_addr: libc::in6_addr {
                            s6_addr: ip.octets(),
                        },
                    };
                    push_cmsg(
                        &mut builder,
                        libc::IPPROTO_IPV6,
                        libc::IPV6_PKTINFO,
                        pktinfo,
                    )?;
                }
                #[cfg(windows)]
                {
                    let pktinfo = WinSock::IN6_PKTINFO {
                        ipi6_addr: WinSock::IN6_ADDR {
                            u: WinSock::IN6_ADDR_0 { Byte: ip.octets() },
                        },
                        ipi6_ifindex: 0,
                    };
                    push_cmsg(
                        &mut builder,
                        WinSock::IPPROTO_IPV6,
                        WinSock::IPV6_PKTINFO,
                        pktinfo,
                    )?;
                }
            }
            None => {}
        }

        // GSO
        if let Some(segment_size) = transmit.segment_size
            && segment_size < transmit.size
        {
            #[cfg(linux_all)]
            push_cmsg(
                &mut builder,
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                segment_size as u16,
            )?;
            #[cfg(windows)]
            push_cmsg(
                &mut builder,
                WinSock::IPPROTO_UDP,
                WinSock::UDP_SEND_MSG_SIZE,
                segment_size as u32,
            )?;
            #[cfg(not(any(linux_all, windows)))]
            let _ = segment_size;
        }

        // builder is dropped here; buffer len was already advanced by push calls.

        Ok(control_buf)
    }

    pub async fn send<T: IoBuf>(&self, buffer: T, transmit: &Transmit) -> T {
        let mut control = self
            .construct_control_message(transmit)
            .expect("CMSG_LEN should be large enough");
        let mut buffer = buffer.slice(..transmit.size);

        loop {
            let res;
            BufResult(res, (buffer, control)) = self
                .inner
                .send_msg(buffer, control, transmit.destination)
                .await;

            match res {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => match e.raw_os_error() {
                    #[cfg(unix)]
                    Some(libc::EMSGSIZE) => {}
                    _ => {
                        #[cfg(linux_all)]
                        if matches!(e.raw_os_error(), Some(libc::EIO) | Some(libc::EINVAL))
                            && self.max_gso_segments().get() > 1
                        {
                            self.has_gso_error.store(true, Ordering::Relaxed);
                        }
                        compio_log::info!("failed to send UDP datagram: {e:?}, {transmit:?}");
                    }
                },
            }
            break;
        }

        buffer.into_inner()
    }

    pub fn close(self) -> impl Future<Output = io::Result<()>> {
        self.inner.close()
    }
}

impl Clone for Socket {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            may_fragment: self.may_fragment,
            max_gro_segments: self.max_gro_segments,
            max_gso_segments: self.max_gso_segments,
            has_gso_error: AtomicBool::new(self.has_gso_error.load(Ordering::Relaxed)),
            #[cfg(freebsd)]
            encode_src_ip_v4: self.encode_src_ip_v4,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SocketEntry {
    pub socket: Socket,
    pub local_ip: Option<IpAddr>,
}

#[derive(Debug, Clone)]
pub(crate) struct EndpointRecvPool {
    pool: BufferPool,
    returned: Arc<Event>,
}

impl EndpointRecvPool {
    pub(crate) fn new(max_payload: usize, max_gro_segments: usize) -> io::Result<Self> {
        Self::with_pool_size(max_payload, max_gro_segments, RECV_POOL_SIZE)
    }

    fn with_pool_size(
        max_payload: usize,
        max_gro_segments: usize,
        pool_size: u16,
    ) -> io::Result<Self> {
        let buffer_len = match max_payload.checked_mul(max_gro_segments) {
            Some(len) if len > 0 => len.min(64 * 1024),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "recv pool buffer length must be positive",
                ));
            }
        };
        let pool = runtime::create_buffer_pool::<BoxAllocator>(pool_size, buffer_len, 0)?;

        Ok(Self {
            pool,
            returned: Arc::new(Event::new()),
        })
    }
}

pub(crate) struct RecvMultiStream<S> {
    inner: RecvMsgManagedMultiStream<S>,
}

impl<S> RecvMultiStream<S> {
    fn new(inner: RecvMsgManagedMultiStream<S>) -> Self {
        Self { inner }
    }
}

impl<S: Clone + AsFd + Unpin + 'static> Stream for RecvMultiStream<S> {
    type Item = io::Result<RecvItem>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
            Some(Ok(msg)) => Poll::Ready(Some(RecvBuffer::from_managed_msg(msg))),
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}

pub(crate) struct RecvBuffer {
    buffer: Buffer,
    offset: usize,
    len: usize,
}

impl RecvBuffer {
    fn from_managed_msg(msg: ManagedRecvMsg) -> io::Result<RecvItem> {
        let data = msg.data();
        let len = data.len();
        let data_ptr = data.as_ptr() as usize;
        let remote = msg
            .remote()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing remote address"))?;
        let meta = RecvMeta::from_parts(remote, len, msg.ancillary());

        if msg.flags().contains(ReturnFlags::TRUNC) {
            compio_log::warn!("received truncated UDP datagram");
        }

        let buffer = msg.into_buffer();
        let buffer_ptr = buffer.as_init().as_ptr() as usize;
        let offset = match data_ptr.checked_sub(buffer_ptr) {
            Some(offset) => offset,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid payload offset",
                ));
            }
        };
        if offset + len > buffer.as_init().len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload exceeds receive buffer",
            ));
        }
        Ok((
            meta,
            Self {
                buffer,
                offset,
                len,
            },
        ))
    }
}

impl AsRef<[u8]> for RecvBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_init()[self.offset..self.offset + self.len]
    }
}

pub(crate) type RecvItem = (RecvMeta, RecvBuffer);

pub(crate) struct SharedSocketState {
    pub sockets: Vec<SocketEntry>,
    pub prev_sockets: Vec<Socket>,
    pub recv_tx: Sender<RecvItem>,
    pub stopped: Arc<AtomicBool>,
    pub recv_pool: EndpointRecvPool,
}

pub(crate) type SocketSet = Arc<Mutex<SharedSocketState>>;

pub(crate) fn select_socket(sockets: &SocketSet, src_ip: Option<IpAddr>) -> Socket {
    let sockets = sockets.lock().unwrap();
    if let Some(ip) = src_ip
        && let Some(entry) = sockets.sockets.iter().find(|e| e.local_ip == Some(ip))
    {
        return entry.socket.clone();
    }
    sockets
        .sockets
        .iter()
        .find(|e| e.local_ip.is_none())
        .unwrap_or(&sockets.sockets[0])
        .socket
        .clone()
}

pub(crate) fn spawn_recv_task(sockets: &SocketSet, socket: &Socket) {
    let socket = socket.clone();
    let recv_tx;
    let stopped;
    let recv_pool;
    {
        let shared = sockets.lock().unwrap();
        recv_tx = shared.recv_tx.clone();
        stopped = shared.stopped.clone();
        recv_pool = shared.recv_pool.clone();
    }

    runtime::spawn(async move {
        let mut recv_stream = socket.recv_multi(&recv_pool);
        loop {
            if stopped.load(Ordering::Relaxed) {
                break;
            }
            match recv_stream.next().await {
                Some(Ok((meta, buf))) => {
                    recv_tx.send_async((meta, buf)).await.ok();
                }
                Some(Err(e)) if e.kind() == io::ErrorKind::ConnectionReset => continue,
                Some(Err(_e)) => {
                    compio_log::error!("socket recv error: {_e:?}");
                    break;
                }
                None => {
                    // RecvMsgManagedMultiStream recreates UDP multishot receives internally,
                    // so this is only a defensive restart if the stream ever terminates.
                    recv_stream = socket.recv_multi(&recv_pool);
                }
            }
        }
    })
    .detach();
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};
    #[cfg(unix)]
    use std::os::fd::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::AsRawSocket;
    use std::time::Duration;

    use compio::runtime::Runtime;
    use futures_util::StreamExt;
    use socket2::{Domain, Protocol, Socket as Socket2, Type};

    use super::*;

    fn test_recv_pool(passive: &Socket) -> EndpointRecvPool {
        EndpointRecvPool::new(u16::MAX as usize, passive.max_gro_segments()).unwrap()
    }

    async fn test_send_recv<T: IoBuf>(
        passive: Socket,
        active: Socket,
        content: T,
        transmit: Transmit,
    ) {
        let passive_addr = passive.local_addr().unwrap();
        let active_addr = active.local_addr().unwrap();
        let recv_pool = test_recv_pool(&passive);
        let mut recv_stream = passive.recv_multi(&recv_pool);

        let content = active.send(content, &transmit).await;

        let segment_size = transmit.segment_size.unwrap_or(transmit.size);
        let expected_datagrams = transmit.size / segment_size;
        let mut datagrams = 0;
        while datagrams < expected_datagrams {
            let (meta, buf) = recv_stream.next().await.unwrap().unwrap();
            let segments = meta.len / meta.stride;
            for i in 0..segments {
                assert_eq!(
                    &content.as_init()
                        [(datagrams + i) * segment_size..(datagrams + i + 1) * segment_size],
                    &buf.as_ref()[(i * meta.stride)..((i + 1) * meta.stride)]
                );
            }
            datagrams += segments;

            assert_eq!(meta.ecn, transmit.ecn);

            assert_eq!(meta.remote.port(), active_addr.port());
            for addr in [meta.remote.ip(), meta.local_ip.unwrap()] {
                match (active_addr.is_ipv6(), passive_addr.is_ipv6()) {
                    (_, false) => assert_eq!(addr, Ipv4Addr::LOCALHOST),
                    (false, true) => assert!(
                        addr == Ipv4Addr::LOCALHOST || addr == Ipv4Addr::LOCALHOST.to_ipv6_mapped()
                    ),
                    (true, true) => assert!(
                        addr == Ipv6Addr::LOCALHOST || addr == Ipv4Addr::LOCALHOST.to_ipv6_mapped()
                    ),
                }
            }
        }
        assert_eq!(datagrams, expected_datagrams);
    }

    /// Helper function to create dualstack udp socket.
    /// This is only used for testing.
    fn bind_udp_dualstack() -> io::Result<UdpSocket> {
        #[cfg(unix)]
        use std::os::fd::{FromRawFd, IntoRawFd};
        #[cfg(windows)]
        use std::os::windows::io::{FromRawSocket, IntoRawSocket};

        let socket = Socket2::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_only_v6(false)?;
        socket.bind(&SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0).into())?;

        Runtime::with_current(|r| r.attach(socket.as_raw_fd()))?;
        #[cfg(unix)]
        unsafe {
            Ok(UdpSocket::from_raw_fd(socket.into_raw_fd()))
        }
        #[cfg(windows)]
        unsafe {
            Ok(UdpSocket::from_raw_socket(socket.into_raw_socket()))
        }
    }

    #[compio::test]
    async fn basic() {
        let passive = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let content = b"hello";
        let transmit = Transmit {
            destination: passive.local_addr().unwrap(),
            ecn: None,
            size: content.len(),
            segment_size: None,
            src_ip: None,
        };
        test_send_recv(passive, active, content, transmit).await;
    }

    #[compio::test]
    async fn recv_multi_burst() {
        const PACKETS: usize = 8;

        let passive = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let recv_pool = test_recv_pool(&passive);
        let mut recv_stream = passive.recv_multi(&recv_pool);
        let passive_addr = passive.local_addr().unwrap();

        for i in 0..PACKETS {
            let content = vec![i as u8; 16];
            let transmit = Transmit {
                destination: passive_addr,
                ecn: None,
                size: content.len(),
                segment_size: None,
                src_ip: None,
            };
            active.send(content, &transmit).await;
        }

        let mut received = Vec::new();
        for _ in 0..PACKETS {
            let (_meta, buf) = recv_stream.next().await.unwrap().unwrap();
            received.push(buf.as_ref().to_vec());
        }
        received.sort();

        let expected = (0..PACKETS).map(|i| vec![i as u8; 16]).collect::<Vec<_>>();
        assert_eq!(received, expected);
    }

    #[compio::test]
    async fn recv_multi_waits_when_pool_exhausted() {
        const POOL_SIZE: u16 = 2;

        let passive = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let passive_addr = passive.local_addr().unwrap();
        let recv_pool = EndpointRecvPool::with_pool_size(
            u16::MAX as usize,
            passive.max_gro_segments(),
            POOL_SIZE,
        )
        .unwrap();

        for i in 0..4u8 {
            active
                .send(
                    vec![i; 32],
                    &Transmit {
                        destination: passive_addr,
                        ecn: None,
                        size: 32,
                        segment_size: None,
                        src_ip: None,
                    },
                )
                .await;
        }

        let mut recv_stream = passive.recv_multi(&recv_pool);
        let (_, b1) = recv_stream.next().await.unwrap().unwrap();
        let (_, b2) = recv_stream.next().await.unwrap().unwrap();

        let (tx, rx) = flume::bounded(1);
        runtime::spawn(async move {
            tx.send_async(recv_stream.next().await).await.ok();
        })
        .detach();

        compio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            rx.try_recv().is_err(),
            "recv should block while pool is full"
        );

        drop(b1);
        let third = rx.recv_async().await.unwrap().unwrap().unwrap();
        assert_eq!(third.1.as_ref(), &[2u8; 32]);

        drop(b2);
    }

    #[compio::test]
    async fn release_recv_buffer_notifies_waiter() {
        const POOL_SIZE: u16 = 1;

        let passive = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let recv_pool = EndpointRecvPool::with_pool_size(
            u16::MAX as usize,
            passive.max_gro_segments(),
            POOL_SIZE,
        )
        .unwrap();

        active
            .send(
                b"x".as_slice(),
                &Transmit {
                    destination: passive.local_addr().unwrap(),
                    ecn: None,
                    size: 1,
                    segment_size: None,
                    src_ip: None,
                },
            )
            .await;

        let mut recv_stream = passive.recv_multi(&recv_pool);
        let (_, held) = recv_stream.next().await.unwrap().unwrap();

        let returned = recv_pool.returned.clone();
        let (tx, rx) = flume::bounded(1);
        runtime::spawn(async move {
            returned.listen().await;
            tx.send_async(()).await.ok();
        })
        .detach();

        compio::time::sleep(Duration::from_millis(20)).await;
        assert!(rx.try_recv().is_err());

        drop(held);
        rx.recv_async().await.unwrap();
    }

    #[compio::test]
    #[cfg_attr(any(non_freebsd, solarish), ignore)]
    async fn ecn_v4() {
        let passive = Socket::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()).unwrap();
        for ecn in [EcnCodepoint::Ect0, EcnCodepoint::Ect1] {
            let content = b"hello";
            let transmit = Transmit {
                destination: passive.local_addr().unwrap(),
                ecn: Some(ecn),
                size: content.len(),
                segment_size: None,
                src_ip: None,
            };
            test_send_recv(passive.clone(), active.clone(), content, transmit).await;
        }
    }

    #[compio::test]
    async fn ecn_v6() {
        let passive = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        for ecn in [EcnCodepoint::Ect0, EcnCodepoint::Ect1] {
            let content = b"hello";
            let transmit = Transmit {
                destination: passive.local_addr().unwrap(),
                ecn: Some(ecn),
                size: content.len(),
                segment_size: None,
                src_ip: None,
            };
            test_send_recv(passive.clone(), active.clone(), content, transmit).await;
        }
    }

    #[compio::test]
    #[cfg_attr(non_freebsd, ignore)]
    async fn ecn_dualstack() {
        let passive = Socket::new(bind_udp_dualstack().unwrap()).unwrap();

        let mut dst_v4 = passive.local_addr().unwrap();
        dst_v4.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let mut dst_v6 = dst_v4;
        dst_v6.set_ip(IpAddr::V6(Ipv6Addr::LOCALHOST));

        for (src, dst) in [("[::1]:0", dst_v6), ("127.0.0.1:0", dst_v4)] {
            let active = Socket::new(UdpSocket::bind(src).await.unwrap()).unwrap();

            for ecn in [EcnCodepoint::Ect0, EcnCodepoint::Ect1] {
                let content = b"hello";
                let transmit = Transmit {
                    destination: dst,
                    ecn: Some(ecn),
                    size: content.len(),
                    segment_size: None,
                    src_ip: None,
                };
                test_send_recv(passive.clone(), active.clone(), content, transmit).await;
            }
        }
    }

    #[compio::test]
    #[cfg_attr(any(non_freebsd, solarish), ignore)]
    async fn ecn_v4_mapped_v6() {
        let passive = Socket::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()).unwrap();
        let active = Socket::new(bind_udp_dualstack().unwrap()).unwrap();

        let mut dst_addr = passive.local_addr().unwrap();
        dst_addr.set_ip(IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()));

        for ecn in [EcnCodepoint::Ect0, EcnCodepoint::Ect1] {
            let content = b"hello";
            let transmit = Transmit {
                destination: dst_addr,
                ecn: Some(ecn),
                size: content.len(),
                segment_size: None,
                src_ip: None,
            };
            test_send_recv(passive.clone(), active.clone(), content, transmit).await;
        }
    }

    #[compio::test]
    #[cfg_attr(not(any(linux, windows)), ignore)]
    async fn gso() {
        let passive = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();
        let active = Socket::new(UdpSocket::bind("[::1]:0").await.unwrap()).unwrap();

        let max_segments = active.max_gso_segments().get();
        const SEGMENT_SIZE: usize = 128;
        let content = vec![0xAB; SEGMENT_SIZE * max_segments];

        let transmit = Transmit {
            destination: passive.local_addr().unwrap(),
            ecn: None,
            size: content.len(),
            segment_size: Some(SEGMENT_SIZE),
            src_ip: None,
        };
        test_send_recv(passive, active, content, transmit).await;
    }
}
