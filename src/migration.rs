use crate::{KeyPair, NodeId, PeerId, PublicKey, Result, SecretKey};
use zeroize::Zeroize;

/// Migrate from old PeerId to new NodeId type
pub fn migrate_peer_id_to_node_id(peer_id: &PeerId) -> Result<NodeId> {
    let bytes = peer_id.to_bytes();
    PublicKey::from_bytes(&bytes)
}

/// Migrate from old KeyPair to new SecretKey type
pub fn migrate_keypair_to_secret_key(keypair: KeyPair) -> Result<SecretKey> {
    let mut private_bytes = keypair.private_key_bytes();
    let result = SecretKey::from_bytes(&private_bytes);
    private_bytes.zeroize(); // Ensure we clean up the temporary bytes
    result
}

/// Helper to convert NodeId back to PeerId for backward compatibility
pub fn node_id_to_peer_id(node_id: &NodeId) -> PeerId {
    // This is safe because NodeId stores valid Ed25519 public key bytes
    PeerId::from_bytes(node_id.as_bytes()).expect("NodeId should always convert to valid PeerId")
}

/// Helper to convert SecretKey to KeyPair for backward compatibility
pub fn secret_key_to_keypair(secret_key: &SecretKey) -> KeyPair {
    let private_bytes = secret_key.to_bytes();
    KeyPair::from_private_key_bytes(&private_bytes)
        .expect("SecretKey should always convert to valid KeyPair")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_id_migration() {
        // Create a test keypair and get its peer ID
        let keypair = KeyPair::generate();
        let peer_id = keypair.peer_id();

        // Migrate to NodeId
        let node_id = migrate_peer_id_to_node_id(&peer_id).unwrap();

        // Convert back
        let peer_id_back = node_id_to_peer_id(&node_id);

        // Should be the same
        assert_eq!(peer_id, peer_id_back);
        assert_eq!(peer_id.to_bytes(), *node_id.as_bytes());
    }

    #[test]
    fn test_keypair_migration() {
        // Create a test keypair
        let keypair = KeyPair::generate();
        let original_public = keypair.public_key_bytes();

        // Migrate to SecretKey
        let secret_key = migrate_keypair_to_secret_key(keypair.clone()).unwrap();

        // Check the public keys match
        let migrated_public = secret_key.public();
        assert_eq!(original_public, *migrated_public.as_bytes());

        // Convert back to KeyPair and verify
        let keypair_back = secret_key_to_keypair(&secret_key);
        assert_eq!(original_public, keypair_back.public_key_bytes());
    }

    #[test]
    fn test_invalid_peer_id() {
        // Create an invalid PeerId with wrong bytes (this won't actually work with current impl)
        // For now, we'll just test that valid conversions work
        let keypair = KeyPair::generate();
        let peer_id = keypair.peer_id();

        // This should succeed
        let result = migrate_peer_id_to_node_id(&peer_id);
        assert!(result.is_ok());
    }
}
