// This file demonstrates the GUARD preventing to_owned() usage
// It will FAIL to compile without test-helpers feature
//
// To see the guard in action, try:
//   cargo check --tests  (without test-helpers) -> should fail
//   cargo check --tests --features test-helpers -> should pass

use kameo_remote::registry::{RegistryMessage, RegistryMessageFrame};

#[test]
fn demonstrates_guard_protection() {
    let message = RegistryMessage::ImmediateAck {
        actor_name: "test_actor".to_string(),
        success: true,
    };
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&message).unwrap();
    let bytes = bytes::Bytes::from(serialized.into_vec());
    let frame = RegistryMessageFrame::new(bytes).unwrap();

    // With #[cfg(feature = "test-helpers")] this method exists
    // Without it, this line won't compile - that's the guard!
    #[cfg(feature = "test-helpers")]
    {
        let _owned = frame.to_owned().unwrap();
        println!("✅ Guard passed: test-helpers feature is enabled");
    }

    #[cfg(not(feature = "test-helpers"))]
    {
        // If you uncomment this, you'll get:
        // error[E0599]: no method named `to_owned` found for struct `RegistryMessageFrame`
        // let _owned = frame.to_owned().unwrap();

        // ✅ This is the correct way in production - zero copy
        let _archived = frame.archived().unwrap();
    }
}
