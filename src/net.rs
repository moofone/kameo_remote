use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tracing::warn;

use crate::GossipConfig;

pub(crate) fn apply_tcp_keepalive(stream: &TcpStream, config: &GossipConfig) {
    let Some(keepalive) = config.tcp_keepalive.as_ref() else {
        return;
    };

    let sock = SockRef::from(stream);
    let ka = TcpKeepalive::new()
        .with_time(keepalive.idle)
        .with_interval(keepalive.interval);

    let _ = keepalive.retries;

    if let Err(e) = sock.set_tcp_keepalive(&ka) {
        warn!(error = %e, "failed to set TCP keepalive");
    }
}
