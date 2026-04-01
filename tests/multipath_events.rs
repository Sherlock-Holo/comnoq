use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use comnoq::{
    Endpoint, PathError, PathEvent, PathId, PathStatus, TransportConfig, n0_nat_traversal,
};
use futures_util::join;

mod common;
use common::{config_pair, subscribe};

async fn endpoint_with_transport(transport: TransportConfig) -> Endpoint {
    let (server_config, client_config) = config_pair(Some(transport));
    let mut endpoint = Endpoint::server("127.0.0.1:0", server_config)
        .await
        .unwrap();
    endpoint.default_client_config = Some(client_config);
    endpoint
}

#[compio::test]
async fn handshake_confirmed_and_open_path_event() {
    let _guard = subscribe();

    let mut transport = TransportConfig::default();
    transport.max_concurrent_multipath_paths(2);
    let endpoint = endpoint_with_transport(transport).await;
    let server_addr = endpoint.local_addr().unwrap();

    let (client, server) = join!(
        async {
            endpoint
                .connect(server_addr, "localhost", None)
                .unwrap()
                .await
                .unwrap()
        },
        async { endpoint.wait_incoming().await.unwrap().await.unwrap() },
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
    endpoint.shutdown().await.unwrap();
}

#[compio::test]
async fn nat_traversal_updates_are_forwarded() {
    let _guard = subscribe();

    let mut transport = TransportConfig::default();
    transport.set_max_remote_nat_traversal_addresses(2);
    let endpoint = endpoint_with_transport(transport).await;
    let server_addr = endpoint.local_addr().unwrap();

    let (client, server) = join!(
        async {
            endpoint
                .connect(server_addr, "localhost", None)
                .unwrap()
                .await
                .unwrap()
        },
        async { endpoint.wait_incoming().await.unwrap().await.unwrap() },
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
    endpoint.shutdown().await.unwrap();
}
