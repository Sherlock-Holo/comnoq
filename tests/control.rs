use std::time::Duration;

use comnoq::{ConnectionError, Endpoint, OpenStreamError, TransportConfig};
use compio::io::AsyncWriteExt;
use synchrony::unsync::async_flag::AsyncFlag;

mod common;
use common::{config_pair, subscribe};
use futures_util::{FutureExt, join};

#[compio::test]
async fn ip_blocking() {
    let _guard = subscribe();

    let (server_config, client_config) = config_pair(None);

    let server = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    let server_addr = server.local_addr().unwrap();

    let client1 = Endpoint::client("127.0.0.1:0").await.unwrap();
    let client1_addr = client1.local_addr().unwrap();
    let client2 = Endpoint::client("127.0.0.1:0").await.unwrap();

    let shutdown_flag = AsyncFlag::new();
    let shutdown_handle = shutdown_flag.handle();

    let srv = compio::runtime::spawn(async move {
        let wait_fut = shutdown_flag.wait().fuse();
        let mut wait_fut = std::pin::pin!(wait_fut);
        loop {
            let incoming = futures_util::select! {
                 incoming = server.accept().fuse() => incoming.unwrap(),
                 _ = wait_fut.as_mut() => break,
            };
            if incoming.remote_address() == client1_addr {
                incoming.refuse();
            } else if incoming.remote_address_validated() {
                incoming.await.unwrap();
            } else {
                incoming.retry().unwrap();
            }
        }
        server.shutdown().await.unwrap();
    });

    let e = client1
        .connect_with(client_config.clone(), server_addr, "localhost")
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(e, ConnectionError::ConnectionClosed(_)));
    client2
        .connect_with(client_config, server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    shutdown_handle.notify();

    srv.await.unwrap();

    client2.shutdown().await.unwrap();
    client1.shutdown().await.unwrap();
}

#[compio::test]
async fn stream_id_flow_control() {
    let _guard = subscribe();

    let mut cfg = TransportConfig::default();
    cfg.max_concurrent_uni_streams(1u32.into());

    let (server_config, client_config) = config_pair(Some(cfg));
    let mut endpoint = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    endpoint.set_default_client_config(client_config);

    let (conn1, conn2) = join!(
        async {
            endpoint
                .connect(endpoint.local_addr().unwrap(), "localhost")
                .unwrap()
                .await
                .unwrap()
        },
        async { endpoint.accept().await.unwrap().await.unwrap() },
    );

    let first = conn1.try_open_uni().unwrap();
    assert!(matches!(
        conn1.try_open_uni(),
        Err(OpenStreamError::StreamsExhausted)
    ));
    drop(first);
    conn2.accept_uni().await.unwrap();

    // If `open_uni` doesn't get unblocked when the previous stream is dropped,
    // this will time out.
    join!(
        async {
            conn1.open_uni().await.unwrap();
        },
        async {
            conn1.open_uni().await.unwrap();
        },
        async {
            conn1.open_uni().await.unwrap();
        },
        async {
            conn2.accept_uni().await.unwrap();
            conn2.accept_uni().await.unwrap();
        }
    );

    drop(conn1);
    drop(conn2);

    endpoint.shutdown().await.unwrap();
}

#[compio::test]
async fn rebind_recv() {
    let _guard = subscribe();

    const MSG: &[u8] = b"hello";

    let mut cfg = TransportConfig::default();
    cfg.max_concurrent_uni_streams(1u32.into());
    let (server_config, client_config) = config_pair(Some(cfg));
    let server_endpoint = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();
    let mut client_endpoint = Endpoint::client("127.0.0.1:0").await.unwrap();
    client_endpoint.set_default_client_config(client_config);

    let write_gate = AsyncFlag::new();
    let write_handle = write_gate.handle();

    let server_task = compio::runtime::spawn(async move {
        let conn = server_endpoint.accept().await.unwrap().await.unwrap();
        write_gate.wait().await;
        let mut stream = conn.open_uni().await.unwrap();
        let (_, msg) = stream.write_all(MSG).await.unwrap();
        assert_eq!(msg, MSG);
        stream.finish().unwrap();
        drop(conn);
        server_endpoint
    });

    let conn = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    client_endpoint
        .rebind(compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap())
        .unwrap();
    write_handle.notify();

    let mut stream = conn.accept_uni().await.unwrap();
    let (_, buf) = stream.read_to_end(vec![]).await.unwrap();
    assert_eq!(buf, MSG);

    conn.close(0u32.into(), b"");
    drop(conn);
    client_endpoint.shutdown().await.unwrap();
    let server_endpoint = server_task.await.unwrap();
    server_endpoint.shutdown().await.unwrap();
}

#[compio::test]
async fn wait_all_draining_after_close() {
    let _guard = subscribe();

    let (server_config, client_config) = config_pair(None);
    let mut endpoint = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    endpoint.set_default_client_config(client_config);
    let addr = endpoint.local_addr().unwrap();

    let (conn1, conn2) = futures_util::join!(
        async { endpoint.connect(addr, "localhost").unwrap().await.unwrap() },
        async { endpoint.accept().await.unwrap().await.unwrap() },
    );

    endpoint.close(0u32.into(), b"");
    futures_util::select! {
        _ = endpoint.wait_all_draining().fuse() => {}
        _ = compio::runtime::time::sleep(Duration::from_secs(1)).fuse() => {
            panic!("wait_all_draining timed out");
        }
    }

    drop(conn1);
    drop(conn2);
    endpoint.shutdown().await.unwrap();
}

#[compio::test]
async fn bidi_stream_id_flow_control() {
    let _guard = subscribe();

    let mut cfg = TransportConfig::default();
    cfg.max_concurrent_bidi_streams(1u32.into());

    let (server_config, client_config) = config_pair(Some(cfg));
    let mut endpoint = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    endpoint.set_default_client_config(client_config);

    let (conn1, conn2) = join!(
        async {
            endpoint
                .connect(endpoint.local_addr().unwrap(), "localhost")
                .unwrap()
                .await
                .unwrap()
        },
        async { endpoint.accept().await.unwrap().await.unwrap() },
    );

    let stream = conn1.try_open_bi().unwrap();
    assert!(matches!(
        conn1.try_open_bi(),
        Err(OpenStreamError::StreamsExhausted)
    ));
    drop(stream);

    drop(conn1);
    drop(conn2);

    endpoint.shutdown().await.unwrap();
}
