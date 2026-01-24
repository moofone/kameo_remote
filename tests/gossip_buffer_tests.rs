use kameo_remote::gossip_buffer::GossipFrameBuffer;

#[test]
fn buffer_alignment_and_growth() {
    let pool = GossipFrameBuffer::new();
    let mut lease = pool.lease(32);
    {
        let slice = lease.as_mut_slice(32);
        for (i, byte) in slice.iter_mut().enumerate() {
            *byte = i as u8;
        }
    }
    let frozen = lease.freeze(32);
    assert_eq!(frozen.as_slice()[0], 0);
    assert_eq!(frozen.as_slice()[31], 31);
    assert_eq!(frozen.as_slice().len(), 32);
}

#[test]
fn pooled_frame_bytes_slice() {
    let pool = GossipFrameBuffer::new();
    let mut lease = pool.lease(64);
    {
        let slice = lease.as_mut_slice(64);
        for (i, byte) in slice.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
    }
    let frozen = lease.freeze_range(16, 32);
    let sub = frozen.slice(8..24);
    assert_eq!(sub.len(), 16);
    assert_eq!(sub.as_slice()[0], 24);
}
