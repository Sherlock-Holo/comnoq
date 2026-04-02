# comnoq

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![crates.io](https://img.shields.io/crates/v/comnoq)](https://crates.io/crates/comnoq)
[![docs.rs](https://img.shields.io/docsrs/comnoq)](https://docs.rs/comnoq)
[![Repository](https://img.shields.io/badge/github-Sherlock--Holo%2Fcomnoq-24292f?logo=github)](https://github.com/Sherlock-Holo/comnoq)

`comnoq` is a fork of `compio-quic`, providing QUIC support for compio with a `noq-proto` backend.

It offers a modern QUIC transport with multiplexed streams, built-in encryption, connection migration, and optional
HTTP/3 support.

## Features

- QUIC client and server support
- Built on `noq-proto`
- Optional HTTP/3 support via the `h3` feature
- Multiple certificate verification options:
  - `platform-verifier`: Use platform-specific certificate verification
  - `native-certs`: Use system's native certificate store
  - `webpki-roots`: Use Mozilla's root certificates
- Integration with compio's completion-based IO model
- Cross-platform support

## Usage

Add the crate directly:

```bash
cargo add comnoq
```

Example:

```rust
use comnoq::{ClientConfig, Endpoint};

let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
let connection = endpoint.connect("example.com:443", "example.com").await?;

// Use the QUIC connection
let (mut send, mut recv) = connection.open_bi().await?;
send.write_all(b"Hello QUIC!").await?;
```
