// This is PRODUCTION code (not in #[cfg(test)] module)
// It should FAIL to compile because to_owned() is guarded

use kameo_remote::registry::RegistryMessageFrame;

fn production_function(frame: &RegistryMessageFrame) {
    // ✅ Production code must use zero-copy access.
    let _archived = frame.archived().unwrap();

    // Demonstrate guarded API only when explicitly enabled.
    #[cfg(feature = "test-helpers")]
    {
        let _owned = frame.to_owned().unwrap();
    }
}

fn main() {
    let bytes = bytes::Bytes::new();
    let frame = RegistryMessageFrame::new(bytes).unwrap();
    production_function(&frame);
}
