#![cfg(any(debug_assertions, feature = "test-helpers"))]

use kameo_remote::{wire_type, GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
struct Ping {
    id: u64,
}

wire_type!(Ping, "kameo.remote.PingTLS");

#[tokio::test]
async fn test_typed_ask_over_tls_with_pooled_path() {
    std::env::set_var("KAMEO_REMOTE_TYPED_ECHO", "1");

    let addr_a: SocketAddr = "127.0.0.1:9011".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9012".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("typed_tls_a");
    let key_pair_b = KeyPair::new_for_testing("typed_tls_b");
    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();
    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    let conn = handle_a.get_connection(addr_b).await.unwrap();
    let request = Ping { id: 42 };
    let response = conn
        .ask_typed_archived::<Ping, Ping>(&request)
        .await
        .unwrap();
    let archived = response.archived().unwrap();
    assert_eq!(archived.id, request.id);

    handle_a.shutdown().await;
    handle_b.shutdown().await;

    std::env::remove_var("KAMEO_REMOTE_TYPED_ECHO");
}

#[tokio::test]
async fn test_typed_tell_over_tls_with_pooled_path() {
    use tokio::time::{Duration, Instant};

    std::env::set_var("KAMEO_REMOTE_TYPED_TELL_CAPTURE", "1");
    kameo_remote::test_helpers::drain_raw_payloads();

    let addr_a: SocketAddr = "127.0.0.1:9013".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9014".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("typed_tls_tell_a");
    let key_pair_b = KeyPair::new_for_testing("typed_tls_tell_b");
    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();
    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(200)).await;
    kameo_remote::test_helpers::drain_raw_payloads();

    let conn = handle_a.get_connection(addr_b).await.unwrap();
    let request = Ping { id: 7 };
    conn.tell_typed(&request).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut decoded_id: Option<u64> = None;
    while Instant::now() < deadline {
        if let Some(payload) =
            kameo_remote::test_helpers::wait_for_raw_payload(Duration::from_millis(200)).await
        {
            if let Ok(archived) = kameo_remote::typed::decode_typed_zero_copy::<Ping>(&payload) {
                decoded_id = Some(archived.id.to_native());
                break;
            }
        }
    }

    if decoded_id.is_none() {
        let payloads = kameo_remote::test_helpers::drain_raw_payloads();
        let lengths: Vec<usize> = payloads.iter().map(|p| p.len()).collect();
        panic!(
            "typed tell payload not captured; saw {} raw payloads with lengths {:?}",
            lengths.len(),
            lengths
        );
    }

    assert_eq!(decoded_id, Some(request.id));

    handle_a.shutdown().await;
    handle_b.shutdown().await;

    std::env::remove_var("KAMEO_REMOTE_TYPED_TELL_CAPTURE");
}
