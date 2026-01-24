use crate::MessageType;

pub const LENGTH_PREFIX_LEN: usize = 4;
pub const REGISTRY_ALIGNMENT: usize = 16; // rkyv requires 16-byte alignment for RegistryMessage

pub const ASK_RESPONSE_HEADER_LEN: usize = 12; // type(1) + correlation_id(2) + pad(9) to ensure 16-byte alignment
pub const GOSSIP_HEADER_LEN: usize = 12; // type(1) + pad(11) to ensure 16-byte payload alignment
pub const ACTOR_HEADER_LEN: usize = 28; // type(1) + correlation_id(2) + reserved(9) + actor_id(8) + type_hash(4) + payload_len(4)
pub const STREAM_HEADER_PREFIX_LEN: usize = 12; // type(1) + correlation_id(2) + pad(9) to align stream header

pub const ASK_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN; // 4 + 12 = 16
pub const GOSSIP_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
pub const ACTOR_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN;

// Verify alignment at compile time
const _: () = {
    assert!(LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN >= REGISTRY_ALIGNMENT);
    assert!((LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN).is_multiple_of(REGISTRY_ALIGNMENT));
    assert!((LENGTH_PREFIX_LEN + STREAM_HEADER_PREFIX_LEN).is_multiple_of(REGISTRY_ALIGNMENT));
};

pub fn write_ask_response_header(
    msg_type: MessageType,
    correlation_id: u16,
    payload_len: usize,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    debug_assert!(matches!(msg_type, MessageType::Ask | MessageType::Response));

    let total_size = ASK_RESPONSE_HEADER_LEN + payload_len;
    let mut header = [0u8; ASK_RESPONSE_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = msg_type as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    // Remaining 9 bytes are padding to ensure 16-byte alignment
    // header[7..16] are already zeroed from initialization
    debug_assert_eq!(header.len(), 16);
    debug_assert_eq!(ASK_RESPONSE_FRAME_HEADER_LEN, 16);
    header
}

pub fn write_gossip_frame_prefix(payload_len: usize) -> [u8; GOSSIP_FRAME_HEADER_LEN] {
    let total_size = GOSSIP_HEADER_LEN + payload_len;
    let mut header = [0u8; GOSSIP_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = MessageType::Gossip as u8;
    // Remaining 11 bytes are padding to ensure 16-byte payload alignment
    // header[5..16] are already zeroed from initialization
    debug_assert_eq!(header.len(), 16);
    debug_assert_eq!(GOSSIP_FRAME_HEADER_LEN, 16);
    header
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_aligned_16(offset: usize) -> bool {
        offset % REGISTRY_ALIGNMENT == 0
    }

    #[test]
    fn gossip_payload_is_16_byte_aligned() {
        // Gossip payload must be 16-byte aligned for RegistryMessage
        let offset = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
        assert_eq!(offset, 16);
        assert!(is_aligned_16(offset));
    }

    #[test]
    fn ask_response_payload_offset_aligned() {
        let offset = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN;
        assert!(is_aligned_16(offset));
    }

    #[test]
    fn actor_payload_offset_aligned() {
        // Wire format: [len:4][type:1][corr:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
        // Payload offset = 4 + 28 = 32, and 32 % 16 = 0 (16-byte aligned!)
        let offset = LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN;
        assert_eq!(offset, 32);
        assert!(is_aligned_16(offset));
    }

    #[test]
    fn stream_header_offset_aligned() {
        // Stream header starts after [len:4][type:1][corr:2][pad:9] = 16
        let offset = LENGTH_PREFIX_LEN + STREAM_HEADER_PREFIX_LEN;
        assert_eq!(offset, 16);
        assert!(is_aligned_16(offset));
    }

    #[test]
    fn codec_trap_length_prefix_stripped_requires_more_padding() {
        // Gossip: type(1) + need 15 more bytes to reach 16
        let gossip_header_without_pad = 1; // type
        let gossip_pad_needed =
            REGISTRY_ALIGNMENT - (LENGTH_PREFIX_LEN + gossip_header_without_pad);
        assert_eq!(gossip_pad_needed, 11); // 4 + 1 + 11 = 16
        assert_eq!(GOSSIP_HEADER_LEN, 12); // type(1) + pad(11)

        // Ask: type(1) + corr(2) = 3, need 9 more to reach 16-byte alignment
        let ask_header_without_pad = 1 + 2;
        let ask_pad_needed = REGISTRY_ALIGNMENT - (LENGTH_PREFIX_LEN + ask_header_without_pad);
        assert_eq!(ask_pad_needed, 9);
        assert_eq!(ASK_RESPONSE_HEADER_LEN, 12); // type(1) + corr(2) + pad(9)

        // For 32-byte aligned actor header, we need 9 reserved bytes
        let actor_header_without_pad = 1 + 2 + 8 + 4 + 4; // 19 bytes
        let actor_header_with_length_prefix = LENGTH_PREFIX_LEN + actor_header_without_pad; // 23
        let actor_pad_needed_for_32 = 32 - actor_header_with_length_prefix;
        assert_eq!(actor_pad_needed_for_32, 9);
    }

    #[test]
    fn write_ask_response_header_sets_length_and_pad() {
        let payload_len = 3;
        let header = write_ask_response_header(MessageType::Ask, 0x1234, payload_len);
        let total = (ASK_RESPONSE_HEADER_LEN + payload_len) as u32;
        assert_eq!(u32::from_be_bytes(header[0..4].try_into().unwrap()), total);
        assert_eq!(header[4], MessageType::Ask as u8);
        assert_eq!(u16::from_be_bytes(header[5..7].try_into().unwrap()), 0x1234);
        assert_eq!(header[7], 0);
    }

    #[test]
    fn write_gossip_frame_prefix_sets_padding() {
        let payload_len = 10;
        let header = write_gossip_frame_prefix(payload_len);
        let total = (GOSSIP_HEADER_LEN + payload_len) as u32;
        assert_eq!(u32::from_be_bytes(header[0..4].try_into().unwrap()), total);
        assert_eq!(header[4], MessageType::Gossip as u8);
        // All remaining bytes should be zero padding
        assert_eq!(&header[5..], &[0u8; 11]);
        assert_eq!(header.len(), 16);
    }

    #[test]
    fn header_lengths_hold_for_varied_payload_sizes() {
        for payload_len in 0..256 {
            let ask_header = write_ask_response_header(MessageType::Ask, 0, payload_len);
            let ask_total = (ASK_RESPONSE_HEADER_LEN + payload_len) as u32;
            assert_eq!(
                u32::from_be_bytes(ask_header[0..4].try_into().unwrap()),
                ask_total
            );

            let gossip_header = write_gossip_frame_prefix(payload_len);
            let gossip_total = (GOSSIP_HEADER_LEN + payload_len) as u32;
            assert_eq!(
                u32::from_be_bytes(gossip_header[0..4].try_into().unwrap()),
                gossip_total
            );

            // All payload offsets must be 16-byte aligned
            assert!(is_aligned_16(LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN));
            assert!(is_aligned_16(LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN));
            assert!(is_aligned_16(LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN));
        }
    }
}
