use ed25519_dalek::VerifyingKey;
use kameo_remote::{KeyPair, PeerId};

fn main() {
    println!("🧪 Testing Ed25519 validation behavior with proper library usage\n");

    // Test 1: Valid keypair from proper generation (should work)
    println!("=== Test 1: Properly Generated Keys ===");
    let valid_keypair = KeyPair::generate();
    let valid_peer_id = valid_keypair.peer_id();
    println!("Generated PeerId: {}", valid_peer_id);

    match valid_peer_id.verifying_key() {
        Ok(_) => println!("✅ Properly generated key is VALID"),
        Err(e) => println!("❌ Generated key rejected: {}", e),
    }

    // Test 2: Try to create VerifyingKey directly from all-zero bytes
    println!("\n=== Test 2: Ed25519-dalek Direct Validation ===");
    let zero_bytes = [0u8; 32];
    match VerifyingKey::from_bytes(&zero_bytes) {
        Ok(_) => println!("❌ Ed25519-dalek accepts all-zero key directly"),
        Err(e) => println!("✅ Ed25519-dalek rejects all-zero key: {}", e),
    }

    // Test 3: Our custom validation wrapper
    println!("\n=== Test 3: Our Custom Validation ===");
    let zero_peer_id = PeerId::from_public_key_bytes([0u8; 32]);
    println!("Zero PeerId: {}", zero_peer_id);

    match zero_peer_id.verifying_key() {
        Ok(_) => println!("❌ Our validation accepts all-zero key"),
        Err(e) => println!("✅ Our validation rejects all-zero key: {}", e),
    }

    // Test 4: Invalid high-bit key
    let mut invalid_bytes = [0u8; 32];
    invalid_bytes[31] = 0x80; // High bit set
    match VerifyingKey::from_bytes(&invalid_bytes) {
        Ok(_) => println!("❌ Ed25519-dalek accepts high-bit key"),
        Err(e) => println!("✅ Ed25519-dalek rejects high-bit key: {}", e),
    }

    let invalid_peer_id = PeerId::from_public_key_bytes(invalid_bytes);
    match invalid_peer_id.verifying_key() {
        Ok(_) => println!("❌ Our validation accepts high-bit key"),
        Err(e) => println!("✅ Our validation rejects high-bit key: {}", e),
    }

    // Test 5: Deterministic keys (like our examples use)
    println!("\n=== Test 5: Deterministic Keys (Seed-based) ===");
    let seed42_keypair = KeyPair::from_seed_for_testing(42);
    let seed42_peer_id = seed42_keypair.peer_id();
    println!("Seed 42 PeerId: {}", seed42_peer_id);

    match seed42_peer_id.verifying_key() {
        Ok(_) => println!("✅ Seed-based key is valid (expected)"),
        Err(e) => println!("❌ Seed-based key rejected: {}", e),
    }

    let seed99_keypair = KeyPair::from_seed_for_testing(99);
    let seed99_peer_id = seed99_keypair.peer_id();
    println!("Seed 99 PeerId: {}", seed99_peer_id);

    match seed99_peer_id.verifying_key() {
        Ok(_) => println!("✅ Seed 99 key is valid (but unauthorized)"),
        Err(e) => println!("❌ Seed 99 key rejected: {}", e),
    }

    println!("\n🔍 Summary:");
    println!("- Ed25519 mathematically allows many keys we consider 'invalid'");
    println!("- Our custom validation layer adds security by rejecting weak patterns");
    println!("- Network-level peer authentication provides the real security");
}
