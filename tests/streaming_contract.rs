use kameo_remote::{
    framing,
    streaming::{self, StreamAssembler, StreamDescriptor, STREAM_DESCRIPTOR_SIZE},
};
use rkyv::{Archive, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn sample_descriptor(payload_len: u64) -> StreamDescriptor {
    StreamDescriptor {
        stream_id: 42,
        payload_len,
        chunk_len: 1024,
        type_hash: 0xABCD1234,
        actor_id: 999,
        flags: 0,
        reserved: 0,
    }
}

#[test]
fn stream_start_frame_matches_layout() {
    let descriptor = sample_descriptor(4096);
    let frame = streaming::encode_stream_start_frame(&descriptor, 0xBEEF);
    assert_eq!(
        frame.len(),
        streaming::STREAM_START_FRAME_LEN,
        "frame length must include descriptor"
    );
    assert_eq!(frame[4], kameo_remote::MessageType::StreamStart as u8);
    assert_eq!(u16::from_be_bytes([frame[5], frame[6]]), 0xBEEF);
    let payload_offset = framing::LENGTH_PREFIX_LEN + framing::STREAM_HEADER_PREFIX_LEN;
    assert_eq!(payload_offset, 16);
    assert_eq!(
        &frame[payload_offset..payload_offset + STREAM_DESCRIPTOR_SIZE],
        &descriptor.to_be_bytes()
    );
}

#[tokio::test]
async fn assembler_emits_completed_payload() {
    let mut assembler = StreamAssembler::new(8192);
    let descriptor = sample_descriptor(8);
    assembler.start_stream(descriptor, Some(7)).unwrap();

    let first_chunk = vec![1, 2, 3, 4];
    let second_chunk = vec![5, 6, 7, 8];
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let writer_task = tokio::spawn({
        let first = first_chunk.clone();
        let second = second_chunk.clone();
        async move {
            writer.write_all(&first).await.unwrap();
            writer.write_all(&second).await.unwrap();
        }
    });

    let first = assembler
        .read_data_direct(&mut reader, first_chunk.len())
        .await
        .unwrap();
    assert!(first.is_none());

    let completion = assembler
        .read_data_direct(&mut reader, second_chunk.len())
        .await
        .unwrap()
        .expect("stream should complete");

    writer_task.await.unwrap();

    assert_eq!(completion.descriptor.stream_id, descriptor.stream_id);
    assert_eq!(completion.payload.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(completion.correlation_id, Some(7));
}

#[tokio::test]
async fn assembler_rejects_excess_bytes() {
    let mut assembler = StreamAssembler::new(16);
    let descriptor = sample_descriptor(4);
    assembler.start_stream(descriptor, None).unwrap();

    let payload = vec![0u8; 8];
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let writer_task = tokio::spawn({
        let bytes = payload.clone();
        async move {
            writer.write_all(&bytes).await.unwrap();
        }
    });

    let err = assembler
        .read_data_direct(&mut reader, payload.len())
        .await
        .unwrap_err();
    writer_task.await.unwrap();
    assert!(matches!(
        err,
        kameo_remote::GossipError::InvalidStreamFrame(_)
    ));
}

#[tokio::test]
async fn direct_read_places_bytes_into_preallocated_buffer() {
    let mut assembler = StreamAssembler::new(128);
    let descriptor = sample_descriptor(32);
    assembler.start_stream(descriptor, Some(5)).unwrap();
    let (ptr_before, len_before) = assembler
        .active_buffer_identity()
        .expect("buffer should exist");

    let payload = vec![0xAC; 32];
    let payload_clone = payload.clone();
    let (mut writer, mut reader) = tokio::io::duplex(256);
    let writer_task = tokio::spawn(async move {
        writer.write_all(&payload_clone).await.unwrap();
    });

    let completed = assembler
        .read_data_direct(&mut reader, payload.len())
        .await
        .unwrap()
        .expect("stream completes with single chunk");
    writer_task.await.unwrap();

    assert_eq!(completed.payload.as_ref(), payload.as_slice());
    assert_eq!(completed.payload.len(), len_before);
    assert_eq!(completed.payload.as_ptr(), ptr_before);
    assert_eq!(
        (completed.payload.as_ptr() as usize) % kameo_remote::framing::REGISTRY_ALIGNMENT,
        0,
        "payload must remain 16-byte aligned"
    );
}

#[tokio::test]
async fn direct_read_without_start_drains_bytes() {
    let mut assembler = StreamAssembler::new(32);
    let payload = vec![0xCC; 12];
    let payload_clone = payload.clone();
    let (mut writer, mut reader) = tokio::io::duplex(256);
    let writer_task = tokio::spawn(async move {
        writer.write_all(&payload_clone).await.unwrap();
    });

    let err = assembler
        .read_data_direct(&mut reader, payload.len())
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        kameo_remote::GossipError::InvalidStreamFrame(_)
    ));
    writer_task.await.unwrap();

    let mut buf = [0u8; 1];
    let eof = reader.read(&mut buf).await.unwrap();
    assert_eq!(eof, 0, "bytes should be drained after error");
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
struct DemoPayload {
    value: u32,
    magic: [u8; 4],
}

#[tokio::test]
async fn completed_payload_is_rkyv_accessible() {
    let payload_struct = DemoPayload {
        value: 1337,
        magic: *b"kimo",
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&payload_struct).unwrap();

    let mut assembler = StreamAssembler::new(bytes.len());
    let descriptor = sample_descriptor(bytes.len() as u64);
    assembler.start_stream(descriptor, None).unwrap();

    let payload_vec = bytes.to_vec();
    let payload_clone = payload_vec.clone();
    let (mut writer, mut reader) = tokio::io::duplex(512);
    let writer_task = tokio::spawn(async move {
        writer.write_all(&payload_clone).await.unwrap();
    });

    let completed = assembler
        .read_data_direct(&mut reader, payload_vec.len())
        .await
        .unwrap()
        .expect("stream completes");
    writer_task.await.unwrap();

    let archived: &<DemoPayload as Archive>::Archived =
        unsafe { &*(completed.payload.as_ptr() as *const <DemoPayload as Archive>::Archived) };
    assert_eq!(archived.value.to_native(), payload_struct.value);
    assert_eq!(archived.magic, payload_struct.magic);
}
