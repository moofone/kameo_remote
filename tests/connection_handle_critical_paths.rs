use bytes::Bytes;
use kameo_remote::connection_pool::{
    BufferConfig, ChannelId, ConnectionHandle, LockFreeStreamHandle,
};
use kameo_remote::typed::{self, WireType};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

async fn build_test_handle() -> ConnectionHandle {
    let (client, mut server) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        loop {
            match server.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    });

    let (stream_handle, writer_task) = LockFreeStreamHandle::new(
        client,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::Global,
        BufferConfig::default(),
    );
    tokio::spawn(async move {
        let _ = writer_task.await;
    });

    ConnectionHandle::from_parts_for_testing(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(stream_handle),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tell_bytes_batch_hits_critical_path() {
    let handle = build_test_handle().await;
    let _guard = kameo_remote::test_helpers::exclusive_zero_copy_capture();
    let msgs = vec![
        Bytes::from_static(b"critical-path-zero-copy-1"),
        Bytes::from_static(b"critical-path-zero-copy-2"),
    ];
    handle
        .tell_bytes_batch(&msgs)
        .await
        .expect("tell_bytes_batch zero-copy");

    let records = kameo_remote::test_helpers::take_zero_copy_records();
    for msg in &msgs {
        let ptr = msg.as_ptr() as usize;
        assert!(
            records
                .iter()
                .any(|record| record.ptr == ptr && record.len == msg.len()),
            "missing zero-copy capture for payload {:?}",
            msg
        );
    }
}

#[derive(Archive, Serialize, Deserialize, Debug)]
struct DummyMessage {
    value: u32,
}

impl WireType for DummyMessage {
    const TYPE_HASH: u64 = typed::fnv1a_hash("DummyMessage");
    const TYPE_NAME: &'static str = "DummyMessage";
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tell_typed_hits_critical_path() {
    let handle = build_test_handle().await;
    let mut payloads = Vec::new();
    for value in 0..3u32 {
        payloads.push(DummyMessage { value });
    }
    handle
        .tell_typed_batch(&payloads)
        .await
        .expect("tell_typed_batch zero-copy");
}
