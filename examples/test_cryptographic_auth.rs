use kameo_remote::{AuthChallenge, AuthResponse, KeyPair};

fn main() {
    println!("🔐 Testing Cryptographic Challenge-Response Authentication\n");

    // Step 1: Generate server and client keypairs
    println!("=== Step 1: Key Generation ===");
    let server_keypair = KeyPair::from_seed_for_testing(42);
    let client_keypair = KeyPair::from_seed_for_testing(43);
    let impostor_keypair = KeyPair::from_seed_for_testing(99);

    let server_peer_id = server_keypair.peer_id();
    let client_peer_id = client_keypair.peer_id();
    let impostor_peer_id = impostor_keypair.peer_id();

    println!("Server PeerId: {}", server_peer_id);
    println!("Client PeerId: {}", client_peer_id);
    println!("Impostor PeerId: {}", impostor_peer_id);

    // Step 2: Server generates challenge
    println!("\n=== Step 2: Server Challenge Generation ===");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let challenge = KeyPair::generate_challenge(timestamp);
    println!(
        "Challenge generated with {} random bytes",
        challenge.challenge.len()
    );

    // Step 3: Legitimate client responds with signature
    println!("\n=== Step 3: Legitimate Client Authentication ===");
    let client_response = client_keypair.create_auth_response(&challenge.challenge, timestamp);
    println!(
        "Client signed challenge, claiming PeerId: {}",
        client_response.peer_id
    );

    match KeyPair::verify_auth_response(&challenge.challenge, &client_response) {
        Ok(()) => println!("✅ LEGITIMATE CLIENT: Authentication successful!"),
        Err(e) => println!("❌ Authentication failed: {}", e),
    }

    // Step 4: Impostor tries to impersonate client
    println!("\n=== Step 4: Impostor Impersonation Attempt ===");
    // Impostor creates response but claims to be the client (wrong PeerId)
    let mut impostor_response =
        impostor_keypair.create_auth_response(&challenge.challenge, timestamp);
    impostor_response.peer_id = client_peer_id; // Claim to be client!

    println!(
        "Impostor signed with their key but claims to be client: {}",
        impostor_response.peer_id
    );

    match KeyPair::verify_auth_response(&challenge.challenge, &impostor_response) {
        Ok(()) => println!("❌ SECURITY BREACH: Impostor authentication succeeded!"),
        Err(e) => println!("✅ IMPOSTOR BLOCKED: Authentication failed: {}", e),
    }

    // Step 5: Valid signature but wrong claimed identity
    println!("\n=== Step 5: Valid Signature, Wrong Identity ===");
    let impostor_honest_response =
        impostor_keypair.create_auth_response(&challenge.challenge, timestamp);
    println!(
        "Impostor with correct PeerId: {}",
        impostor_honest_response.peer_id
    );

    // This should succeed cryptographically but fail authorization (server doesn't expect this PeerId)
    match KeyPair::verify_auth_response(&challenge.challenge, &impostor_honest_response) {
        Ok(()) => {
            println!("✅ Cryptographically valid (but server should reject unauthorized PeerId)")
        }
        Err(e) => println!("❌ Unexpected cryptographic failure: {}", e),
    }

    println!("\n🔍 === Security Analysis ===");
    println!("✅ Legitimate clients can prove key ownership");
    println!("✅ Impostors cannot forge signatures for other identities");
    println!("✅ Cryptographic verification prevents identity theft");
    println!("⚠️  Server must still check if authenticated PeerId is authorized");
}
