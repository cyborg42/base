//! WebSocket subscription handling for flashblocks.

use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use base_common_flashblocks::Flashblock;
use futures::{SinkExt as _, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{Instant, interval_at},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async,
    tungstenite::{Error as WsError, error::UrlError, protocol::Message},
};
use url::Url;

use crate::{FlashblockSequenceGate, FlashblocksReceiver, SequenceGateDecision, metrics::Metrics};

#[derive(Debug)]
enum ActorMessage {
    BestPayload { upstream: usize, payload: Flashblock },
}

/// Subscribes to flashblocks via WebSocket and forwards them to the receiver.
///
/// With several upstream URLs, one connection per URL feeds a [`FlashblockSequenceGate`] that
/// passes on each flashblock from whichever connection delivers it first.
#[derive(Debug)]
pub struct FlashblocksSubscriber<Receiver> {
    flashblocks_state: Arc<Receiver>,
    ws_urls: Vec<Url>,
    ping_interval: Duration,
}

impl<Receiver> FlashblocksSubscriber<Receiver>
where
    Receiver: FlashblocksReceiver + Send + Sync + 'static,
{
    /// Max duration of backoff before reconnecting to upstream.
    pub const MAX_BACKOFF: Duration = Duration::from_secs(10);

    /// Creates a new flashblocks subscriber.
    pub const fn new(
        flashblocks_state: Arc<Receiver>,
        ws_urls: Vec<Url>,
        ping_interval: Duration,
    ) -> Self {
        Self { ws_urls, flashblocks_state, ping_interval }
    }

    /// Starts the WebSocket subscription to receive flashblocks.
    pub fn start(&mut self) {
        let (sender, mut mailbox) = mpsc::channel(100);
        let upstreams = self.ws_urls.len();
        for (upstream, ws_url) in self.ws_urls.iter().enumerate() {
            info!(
                message = "Starting Flashblocks subscription",
                url = %ws_url,
                upstream,
            );
            Self::spawn_upstream(
                upstream,
                upstreams,
                ws_url.clone(),
                self.ping_interval,
                sender.clone(),
            );
        }

        let flashblocks_state = Arc::clone(&self.flashblocks_state);
        tokio::spawn(async move {
            // A single upstream is already in order; the gate would only change how gaps
            // reach the processor.
            let mut gate = (upstreams > 1).then(FlashblockSequenceGate::default);
            while let Some(message) = mailbox.recv().await {
                match message {
                    ActorMessage::BestPayload { upstream, payload } => {
                        if let Some(gate) = gate.as_mut() {
                            match gate.admit(&payload) {
                                SequenceGateDecision::Forward => {
                                    Metrics::upstream_first_arrivals(upstream.to_string())
                                        .increment(1);
                                }
                                SequenceGateDecision::Duplicate => {
                                    Metrics::upstream_duplicates().increment(1);
                                    continue;
                                }
                                SequenceGateDecision::OutOfSequence => {
                                    Metrics::upstream_out_of_sequence().increment(1);
                                    continue;
                                }
                            }
                        }
                        flashblocks_state.on_flashblock_received(payload);
                    }
                }
            }
        });
    }

    /// Spawns the connection task for one upstream, which reconnects with backoff forever.
    fn spawn_upstream(
        upstream: usize,
        upstreams: usize,
        ws_url: Url,
        ping_period: Duration,
        sender: mpsc::Sender<ActorMessage>,
    ) {
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);

            loop {
                match Self::connect(upstream, upstreams, &ws_url).await {
                    Ok(ws_stream) => {
                        backoff = Duration::from_secs(1);

                        let mut ping_interval =
                            interval_at(Instant::now() + ping_period, ping_period);
                        let mut awaiting_pong_resp = false;

                        let (mut write, mut read) = ws_stream.split();

                        'conn: loop {
                            tokio::select! {
                                Some(msg) = read.next() => {
                                    Metrics::upstream_messages().increment(1);

                                    match msg {
                                        Ok(msg @ (Message::Binary(_) | Message::Text(_))) => {
                                            let bytes = msg.into_data();
                                            match Flashblock::try_decode_message(bytes) {
                                                Ok(payload) => {
                                                    let _ = sender.send(ActorMessage::BestPayload { upstream, payload }).await.map_err(|e| {
                                                        error!(message = "Failed to publish message to channel", error = %e);
                                                    });
                                                }
                                                Err(e) => {
                                                    error!(
                                                        message = "error decoding flashblock message",
                                                        error = %e
                                                    );
                                                }
                                            }
                                        }
                                        Ok(Message::Close(_)) => {
                                            info!(message = "WebSocket connection closed by upstream");
                                            break;
                                        }
                                        Ok(Message::Pong(data)) => {
                                            trace!(target: "flashblocks_rpc::subscription",
                                                ?data,
                                                "Received pong from upstream"
                                            );
                                            awaiting_pong_resp = false
                                        }
                                        Err(e) => {
                                            Metrics::upstream_errors().increment(1);
                                            error!(
                                                message = "error receiving message",
                                                error = %e
                                            );
                                            break;
                                        }
                                        _ => {}
                                    }
                                },
                                _ = ping_interval.tick() => {
                                    if awaiting_pong_resp {
                                          warn!(
                                            target: "flashblocks_rpc::subscription",
                                            ?backoff,
                                            timeout = ?ping_period,
                                            "No pong response from upstream, reconnecting",
                                        );

                                        backoff = Self::sleep(backoff).await;
                                        break 'conn;
                                    }

                                    trace!(target: "flashblocks_rpc::subscription",
                                        "Sending ping to upstream"
                                    );

                                    if let Err(error) = write.send(Message::Ping(Default::default())).await {
                                        warn!(
                                            target: "flashblocks_rpc::subscription",
                                            ?backoff,
                                            %error,
                                            "WebSocket connection lost, reconnecting",
                                        );

                                        backoff = Self::sleep(backoff).await;
                                        break 'conn;
                                    }
                                    awaiting_pong_resp = true
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            message = "WebSocket connection error, retrying",
                            backoff_duration = ?backoff,
                            error = %e
                        );

                        backoff = Self::sleep(backoff).await;
                        continue;
                    }
                }
            }
        });
    }

    /// Opens the WebSocket connection for one upstream.
    ///
    /// A single upstream connects exactly as before. With several, the host is resolved on every
    /// attempt and upstream `i` starts at resolved address `i mod n`: a plain connect keeps the
    /// first address that answers, which would put every connection on the same edge address,
    /// while the late-delivery independence that motivates several connections was measured
    /// across distinct addresses. TLS SNI and the Host header still come from the URL.
    async fn connect(
        upstream: usize,
        upstreams: usize,
        ws_url: &Url,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, WsError> {
        if upstreams == 1 {
            let (ws_stream, _) = connect_async(ws_url.as_str()).await?;
            info!(message = "WebSocket connection established");
            return Ok(ws_stream);
        }

        let host = ws_url
            .host_str()
            .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
            .ok_or(WsError::Url(UrlError::NoHostName))?;
        let port =
            ws_url.port_or_known_default().ok_or(WsError::Url(UrlError::UnsupportedUrlScheme))?;
        let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
        addrs.sort_unstable();
        addrs.dedup();

        let mut last_error =
            io::Error::new(io::ErrorKind::NotFound, "upstream host resolved to no addresses");
        let start = upstream % addrs.len().max(1);
        for addr in addrs.iter().cycle().skip(start).take(addrs.len()) {
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    let (ws_stream, _) =
                        client_async_tls_with_config(ws_url.as_str(), stream, None, None).await?;
                    info!(
                        message = "WebSocket connection established",
                        upstream,
                        url = %ws_url,
                        peer = %addr,
                    );
                    return Ok(ws_stream);
                }
                Err(error) => {
                    warn!(
                        target: "flashblocks_rpc::subscription",
                        upstream,
                        peer = %addr,
                        %error,
                        "Upstream address unreachable, trying the next one",
                    );
                    last_error = error;
                }
            }
        }
        Err(WsError::Io(last_error))
    }

    /// Sleeps for given backoff duration. Returns incremented backoff duration, capped at [`MAX_BACKOFF`].
    async fn sleep(backoff: Duration) -> Duration {
        Metrics::reconnect_attempts().increment(1);
        tokio::time::sleep(backoff).await;
        std::cmp::min(backoff * 2, Self::MAX_BACKOFF)
    }
}
