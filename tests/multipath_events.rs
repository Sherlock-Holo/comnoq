use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use comnoq::{
    Endpoint, PathError, PathEvent, PathId, PathStatus, TransportConfig, n0_nat_traversal,
};
use futures_util::{FutureExt, StreamExt, join};

mod common;
use common::{config_pair, subscribe};

async fn endpoint_pair_with_transport(transport: TransportConfig) -> (Endpoint, Endpoint) {
    let (server_config, client_config) = config_pair(Some(transport));
    let server = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    let mut client = Endpoint::client("127.0.0.1:0").await.unwrap();
    client.default_client_config = Some(client_config);
    (client, server)
}

#[cfg(linux_all)]
async fn dual_stack_endpoint_pair_with_transport(
    transport: TransportConfig,
) -> (Endpoint, Endpoint) {
    let (server_config, client_config) = config_pair(Some(transport));
    let server = Endpoint::server("[::]:0", server_config).await.unwrap();
    let mut client = Endpoint::client("[::]:0").await.unwrap();
    client.default_client_config = Some(client_config);
    (client, server)
}

#[compio::test]
async fn path_api() {
    let _guard = subscribe();

    let (client_endpoint, server_endpoint) =
        endpoint_pair_with_transport(TransportConfig::default()).await;
    let server_addr = server_endpoint.local_addr().unwrap();

    let (client, server) = join!(
        async {
            client_endpoint
                .connect(server_addr, "localhost", None)
                .unwrap()
                .await
                .unwrap()
        },
        async {
            server_endpoint
                .wait_incoming()
                .await
                .unwrap()
                .await
                .unwrap()
        },
    );

    let path = client.path(PathId::ZERO).expect("path zero exists");
    assert_eq!(path, client.path(PathId::ZERO).unwrap());
    assert_eq!(path.remote_address().unwrap(), client.remote_address());

    let idle_timeout = Duration::from_millis(250);
    let next_idle_timeout = Duration::from_millis(500);
    let previous_idle_timeout = path.set_max_idle_timeout(Some(idle_timeout)).unwrap();
    assert_eq!(
        path.set_max_idle_timeout(Some(next_idle_timeout)).unwrap(),
        Some(idle_timeout)
    );
    path.set_max_idle_timeout(previous_idle_timeout).unwrap();

    let keep_alive = Duration::from_millis(100);
    let next_keep_alive = Duration::from_millis(200);
    let previous_keep_alive = path.set_keep_alive_interval(Some(keep_alive)).unwrap();
    assert_eq!(
        path.set_keep_alive_interval(Some(next_keep_alive)).unwrap(),
        Some(keep_alive)
    );
    path.set_keep_alive_interval(previous_keep_alive).unwrap();

    path.ping().unwrap();

    let mut discovery = path.observed_external_addr().unwrap();
    let first_poll = discovery.next().now_or_never();
    assert!(
        !matches!(first_poll, Some(None)),
        "address discovery stream should not terminate immediately"
    );

    drop(discovery);
    drop(path);
    drop(server);
    drop(client);
    client_endpoint.shutdown().await.unwrap();
    server_endpoint.shutdown().await.unwrap();
}

#[compio::test]
async fn handshake_confirmed_and_open_path_event() {
    let _guard = subscribe();

    let mut transport = TransportConfig::default();
    transport.max_concurrent_multipath_paths(2);
    let (client_endpoint, server_endpoint) = endpoint_pair_with_transport(transport).await;
    let server_addr = server_endpoint.local_addr().unwrap();

    let (client, server) = join!(
        async {
            client_endpoint
                .connect(server_addr, "localhost", None)
                .unwrap()
                .await
                .unwrap()
        },
        async {
            server_endpoint
                .wait_incoming()
                .await
                .unwrap()
                .await
                .unwrap()
        },
    );

    client.handshake_confirmed().await.unwrap();
    assert!(client.is_multipath_enabled());

    let path_events = client.path_events();
    let path = loop {
        match client.open_path(server_addr, PathStatus::Available).await {
            Ok(path) => break path,
            Err(PathError::RemoteCidsExhausted) => {
                compio::runtime::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("unexpected open_path error: {err:?}"),
        }
    };

    assert_ne!(path.id(), PathId::ZERO);
    assert!(path.stats().is_some());

    loop {
        let event = path_events.recv_async().await.unwrap();
        if matches!(event, PathEvent::Opened { id } if id == path.id()) {
            break;
        }
    }

    drop(path);
    drop(server);
    drop(client);
    client_endpoint.shutdown().await.unwrap();
    server_endpoint.shutdown().await.unwrap();
}

#[cfg(linux_all)]
#[compio::test]
async fn handshake_confirmed_and_open_path_event_dual_stack() {
    let _guard = subscribe();

    let mut transport = TransportConfig::default();
    transport.max_concurrent_multipath_paths(2);
    let (client_endpoint, server_endpoint) =
        dual_stack_endpoint_pair_with_transport(transport).await;
    let mut ipv4_server_addr = server_endpoint.local_addr().unwrap();
    ipv4_server_addr.set_ip("127.0.0.1".parse().unwrap());

    let (client, server) = join!(
        async {
            client_endpoint
                .connect(ipv4_server_addr, "localhost", None)
                .unwrap()
                .await
                .unwrap()
        },
        async {
            server_endpoint
                .wait_incoming()
                .await
                .unwrap()
                .await
                .unwrap()
        },
    );

    client.handshake_confirmed().await.unwrap();
    assert!(client.is_multipath_enabled());

    let path_events = client.path_events();
    let mut ipv6_server_addr = ipv4_server_addr;
    ipv6_server_addr.set_ip("::1".parse().unwrap());
    let path = loop {
        match client
            .open_path(ipv6_server_addr, PathStatus::Available)
            .await
        {
            Ok(path) => break path,
            Err(PathError::RemoteCidsExhausted) => {
                compio::runtime::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("unexpected open_path error: {err:?}"),
        }
    };

    assert_ne!(path.id(), PathId::ZERO);
    assert!(path.stats().is_some());

    loop {
        let event = path_events.recv_async().await.unwrap();
        if matches!(event, PathEvent::Opened { id } if id == path.id()) {
            break;
        }
    }

    drop(path);
    drop(server);
    drop(client);
    client_endpoint.shutdown().await.unwrap();
    server_endpoint.shutdown().await.unwrap();
}

#[compio::test]
async fn nat_traversal_updates_are_forwarded() {
    let _guard = subscribe();

    let mut transport = TransportConfig::default();
    transport.set_max_remote_nat_traversal_addresses(2);
    let (client_endpoint, server_endpoint) = endpoint_pair_with_transport(transport).await;
    let server_addr = server_endpoint.local_addr().unwrap();

    let (client, server) = join!(
        async {
            client_endpoint
                .connect(server_addr, "localhost", None)
                .unwrap()
                .await
                .unwrap()
        },
        async {
            server_endpoint
                .wait_incoming()
                .await
                .unwrap()
                .await
                .unwrap()
        },
    );

    client.handshake_confirmed().await.unwrap();

    let updates = client.nat_traversal_updates();
    let added_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 9));
    server.add_nat_traversal_address(added_addr).unwrap();

    let event = updates.recv_async().await.unwrap();
    assert!(matches!(event, n0_nat_traversal::Event::AddressAdded(addr) if addr == added_addr));
    assert_eq!(
        client.get_remote_nat_traversal_addresses().unwrap(),
        vec![added_addr]
    );

    drop(server);
    drop(client);
    client_endpoint.shutdown().await.unwrap();
    server_endpoint.shutdown().await.unwrap();
}
