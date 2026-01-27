use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, MutexGuard, OnceLock,
};
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};

struct RawCapture {
    messages: Mutex<Vec<Bytes>>,
    notify: Notify,
}

fn raw_capture() -> &'static RawCapture {
    static CAPTURE: OnceLock<RawCapture> = OnceLock::new();
    CAPTURE.get_or_init(|| RawCapture {
        messages: Mutex::new(Vec::new()),
        notify: Notify::new(),
    })
}

pub fn record_raw_payload(payload: Bytes) {
    let capture = raw_capture();
    {
        let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
        guard.push(payload);
    }
    capture.notify.notify_waiters();
}

pub fn drain_raw_payloads() -> Vec<Bytes> {
    let capture = raw_capture();
    let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
    guard.drain(..).collect()
}

pub async fn wait_for_raw_payload(timeout: Duration) -> Option<Bytes> {
    let capture = raw_capture();
    {
        let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
        if let Some(payload) = guard.pop() {
            return Some(payload);
        }
    }

    tokio::select! {
        _ = capture.notify.notified() => {
            let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
            guard.pop()
        }
        _ = sleep(timeout) => None,
    }
}

struct ZeroCopyCapture {
    records: Mutex<Vec<ZeroCopyRecord>>,
}

fn zero_copy_capture() -> &'static ZeroCopyCapture {
    static CAPTURE: OnceLock<ZeroCopyCapture> = OnceLock::new();
    CAPTURE.get_or_init(|| ZeroCopyCapture {
        records: Mutex::new(Vec::new()),
    })
}

static ZERO_COPY_ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
pub struct ZeroCopyRecord {
    pub ptr: usize,
    pub len: usize,
    pub payload: Bytes,
}

pub fn enable_zero_copy_capture() {
    ZERO_COPY_ENABLED.store(true, Ordering::SeqCst);
    zero_copy_capture()
        .records
        .lock()
        .expect("zero-copy mutex poisoned")
        .clear();
}

pub fn disable_zero_copy_capture() {
    ZERO_COPY_ENABLED.store(false, Ordering::SeqCst);
    zero_copy_capture()
        .records
        .lock()
        .expect("zero-copy mutex poisoned")
        .clear();
}

pub struct ZeroCopyCaptureGuard {
    _guard: MutexGuard<'static, ()>,
}

impl Drop for ZeroCopyCaptureGuard {
    fn drop(&mut self) {
        disable_zero_copy_capture();
    }
}

pub fn exclusive_zero_copy_capture() -> ZeroCopyCaptureGuard {
    static CAPTURE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let mutex = CAPTURE_LOCK.get_or_init(|| Mutex::new(()));
    let guard = mutex.lock().expect("zero-copy capture lock poisoned");
    enable_zero_copy_capture();
    ZeroCopyCaptureGuard { _guard: guard }
}

pub fn record_zero_copy_payload(payload: Bytes) {
    if !ZERO_COPY_ENABLED.load(Ordering::Relaxed) {
        return;
    }

    let record = ZeroCopyRecord {
        ptr: payload.as_ptr() as usize,
        len: payload.len(),
        payload,
    };

    zero_copy_capture()
        .records
        .lock()
        .expect("zero-copy mutex poisoned")
        .push(record);
}

pub fn take_zero_copy_records() -> Vec<ZeroCopyRecord> {
    zero_copy_capture()
        .records
        .lock()
        .expect("zero-copy mutex poisoned")
        .drain(..)
        .collect()
}
