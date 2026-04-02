//! QUIC implementation based on [`quinn-proto`].
//!
//! [`quinn-proto`]: https://docs.rs/quinn-proto

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]
#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![doc(
    html_logo_url = "https://github.com/compio-rs/compio-logo/raw/refs/heads/master/generated/colored-bold.svg"
)]
#![doc(
    html_favicon_url = "https://github.com/compio-rs/compio-logo/raw/refs/heads/master/generated/colored-bold.svg"
)]

#[cfg(feature = "qlog")]
pub use noq_proto::QlogConfig;
pub use noq_proto::{
    AckFrequencyConfig, ApplicationClose, Chunk, ClientConfig, ClosePathError, ClosedPath,
    ClosedStream, ConfigError, ConnectError, ConnectionClose, ConnectionId, ConnectionIdGenerator,
    ConnectionStats, Dir, EcnCodepoint, EndpointConfig, FrameStats, FrameType, IdleTimeout,
    MtuDiscoveryConfig, MultipathNotNegotiated, NoneTokenLog, NoneTokenStore, PathAbandonReason,
    PathError, PathEvent, PathId, PathStats, PathStatus, ServerConfig, SetPathStatusError, Side,
    StdSystemTime, StreamId, TimeSource, TokenLog, TokenMemoryCache, TokenReuseError, TokenStore,
    Transmit, TransportConfig, TransportErrorCode, UdpStats, ValidationTokenConfig, VarInt,
    VarIntBoundsExceeded, Written, congestion, crypto, n0_nat_traversal,
};

#[cfg(rustls)]
mod builder;
mod connection;
mod endpoint;
mod incoming;
mod path;
mod recv_stream;
mod send_stream;
mod socket;

#[cfg(rustls)]
pub use builder::{ClientBuilder, ServerBuilder};
pub use connection::{Connecting, Connection, ConnectionError, OpenStreamError, SendDatagramError};
pub use endpoint::{Endpoint, EndpointStats};
pub use incoming::{Incoming, IncomingFuture};
pub use path::{OpenPath, Path};
pub use recv_stream::{ReadError, ReadExactError, RecvStream, ResetError};
pub use send_stream::{SendStream, StoppedError, WriteError};
#[cfg(feature = "sync")]
pub(crate) use synchrony::sync;
#[cfg(not(feature = "sync"))]
pub(crate) use synchrony::unsync as sync;

pub(crate) use crate::{
    connection::{ConnectionEvent, ConnectionInner},
    endpoint::EndpointRef,
    socket::*,
};

/// HTTP/3 support via [`h3`].
#[cfg(feature = "h3")]
pub mod h3 {
    #[doc(no_inline)]
    pub use h3::*;

    pub use crate::{
        connection::h3_impl::{BidiStream, OpenStreams},
        send_stream::h3_impl::SendStream,
    };
}
