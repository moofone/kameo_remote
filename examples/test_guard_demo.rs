// DEMONSTRATION: Feature Guard for to_owned()
//
// This file demonstrates how the guard prevents accidental allocation
//
// COMPILE THIS TO SEE THE GUARD IN ACTION:
//   rustc --edition 2021 test_guard_demo.rs --extern kameo_remote=target/debug/libkameo_remote.rlib

use kameo_remote::registry::RegistryMessageFrame;

#[cfg(feature = "test-helpers")]
fn test_code(frame: &RegistryMessageFrame) {
    // ✅ This compiles because test-helpers feature is enabled
    let _owned = frame.to_owned().unwrap();
    println!("Test code: to_owned() is available");
}

#[cfg(not(feature = "test-helpers"))]
fn production_code(frame: &RegistryMessageFrame) {
    // ❌ TRY TO UNCOMMENT THE LINE BELOW - it will NOT compile!
    // let _owned = frame.to_owned().unwrap();
    //
    // Error you'll see:
    //   error[E0599]: no method named `to_owned` found for struct `RegistryMessageFrame`
    //
    // This is the GUARD in action - preventing accidental allocation!

    // ✅ Production code MUST use zero-copy access instead
    let _archived = frame.archived().unwrap();
    println!("Production code: using zero-copy access");
}

fn main() {
    let bytes = bytes::Bytes::new();
    let frame = RegistryMessageFrame::new(bytes).unwrap();

    #[cfg(feature = "test-helpers")]
    {
        test_code(&frame);
    }

    #[cfg(not(feature = "test-helpers"))]
    {
        production_code(&frame);
    }
}
