//! Helpers for Base-specific RPC implementations.

use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_json_rpc::{RpcRecv, RpcSend};
use alloy_primitives::{B256, U64, hex};
use alloy_rpc_client::{BuiltInConnectionString, ClientBuilder, RpcClient as Client};
use alloy_transport_http::{Http, reqwest as alloy_reqwest};
use reth_tasks::Runtime;
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tracing::{debug, trace, warn};

use crate::{SequencerClientError, metrics::SequencerMetrics};

/// Sequencer client error
#[derive(Error, Debug)]
pub enum Error {
    /// Invalid scheme
    #[error("Invalid scheme of sequencer url: {0}")]
    InvalidScheme(String),
    /// Invalid header or value provided.
    #[error("Invalid header: {0}")]
    InvalidHeader(String),
    /// Invalid url
    #[error("Invalid sequencer url: {0}")]
    InvalidUrl(String),
    /// Establishing a connection to the sequencer endpoint resulted in an error.
    #[error("Failed to connect to sequencer: {0}")]
    TransportError(
        #[from]
        #[source]
        alloy_transport::TransportError,
    ),
    /// Reqwest failed to init client
    #[error("Failed to init reqwest client for sequencer: {0}")]
    ReqwestError(
        #[from]
        #[source]
        alloy_transport_http::reqwest::Error,
    ),
}

/// A client to interact with a Sequencer
#[derive(Debug, Clone)]
pub struct SequencerClient {
    inner: Arc<SequencerClientInner>,
}

impl SequencerClientInner {
    /// Creates a new instance with the given endpoint and client.
    pub const fn new(sequencer_endpoint: String, client: Client) -> Self {
        Self { sequencer_endpoint, client }
    }
}

impl SequencerClient {
    /// Interval between keepalive requests on an HTTP sequencer connection.
    ///
    /// Cloudflare in front of `mainnet-sequencer.base.org` closes idle keep-alive connections
    /// after 400 s (measured 2026-10-16); a forward that finds no open connection pays DNS, TCP
    /// and TLS first, about 15-20 ms on top of a warm round trip.
    pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

    /// Upper bound on one keepalive request, so a hung request cannot stall the cadence.
    pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Creates a new [`SequencerClient`] for the given URL.
    ///
    /// If the URL is a websocket endpoint we connect a websocket instance.
    pub async fn new(sequencer_endpoint: impl Into<String>) -> Result<Self, Error> {
        Self::new_with_headers(sequencer_endpoint, Default::default()).await
    }

    /// Creates a new `SequencerClient` for the given URL with the given headers
    ///
    /// This expects headers in the form: `header=value`
    pub async fn new_with_headers(
        sequencer_endpoint: impl Into<String>,
        headers: Vec<String>,
    ) -> Result<Self, Error> {
        let sequencer_endpoint = sequencer_endpoint.into();
        let endpoint = BuiltInConnectionString::from_str(&sequencer_endpoint)?;
        if matches!(&endpoint, BuiltInConnectionString::Http(_)) {
            Self::new_http_with_headers(sequencer_endpoint, headers)
        } else {
            let client = ClientBuilder::default().connect_with(endpoint).await?;
            let inner = SequencerClientInner::new(sequencer_endpoint, client);
            Ok(Self { inner: Arc::new(inner) })
        }
    }

    /// Creates an HTTP sequencer client synchronously, with optional `header=value` headers.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-HTTP URL, invalid headers, or HTTP client construction.
    pub fn new_http_with_headers(
        sequencer_endpoint: impl Into<String>,
        headers: Vec<String>,
    ) -> Result<Self, Error> {
        let sequencer_endpoint = sequencer_endpoint.into();
        if !matches!(
            BuiltInConnectionString::from_str(&sequencer_endpoint)?,
            BuiltInConnectionString::Http(_)
        ) {
            return Err(Error::InvalidScheme(sequencer_endpoint));
        }
        // reqwest evicts connections idle for 90 s by default, and forwards are often further
        // apart than that, so most of them would reconnect. Server-side idle closes are handled
        // by `spawn_keepalive`.
        let mut builder = alloy_reqwest::Client::builder().use_rustls_tls().pool_idle_timeout(None);
        if !headers.is_empty() {
            let mut header_map = alloy_reqwest::header::HeaderMap::new();
            for header in headers {
                if let Some((key, value)) = header.split_once('=') {
                    header_map.insert(
                        key.trim()
                            .parse::<alloy_reqwest::header::HeaderName>()
                            .map_err(|err| Error::InvalidHeader(err.to_string()))?,
                        value
                            .trim()
                            .parse::<alloy_reqwest::header::HeaderValue>()
                            .map_err(|err| Error::InvalidHeader(err.to_string()))?,
                    );
                }
            }
            builder = builder.default_headers(header_map);
        }
        Self::with_http_client(sequencer_endpoint, builder.build()?)
    }

    /// Creates a new [`SequencerClient`] with http transport with the given http client.
    pub fn with_http_client(
        sequencer_endpoint: impl Into<String>,
        client: alloy_reqwest::Client,
    ) -> Result<Self, Error> {
        let sequencer_endpoint: String = sequencer_endpoint.into();
        let url = sequencer_endpoint
            .parse()
            .map_err(|_| Error::InvalidUrl(sequencer_endpoint.clone()))?;

        let http_client = Http::with_client(client, url);
        let is_local = http_client.guess_local();
        let client = ClientBuilder::default().transport(http_client, is_local);

        let inner = SequencerClientInner::new(sequencer_endpoint, client);
        Ok(Self { inner: Arc::new(inner) })
    }

    /// Returns the sequencer endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.inner.sequencer_endpoint
    }

    /// Returns the client
    pub fn client(&self) -> &Client {
        &self.inner.client
    }

    /// Sends a [`alloy_rpc_client::RpcCall`] request to the sequencer endpoint.
    pub async fn request<Params: RpcSend, Resp: RpcRecv>(
        &self,
        method: &str,
        params: Params,
    ) -> Result<Resp, SequencerClientError> {
        Ok(self.client().request::<Params, Resp>(method.to_string(), params).await.inspect_err(
            |err| {
                warn!(
                    target: "rpc::sequencer",
                    %err,
                    "HTTP request to sequencer failed",
                );
            },
        )?)
    }

    /// Spawns a task that keeps the HTTP connection to the sequencer open.
    ///
    /// The task stops on node shutdown or once every handle to this client is dropped.
    /// WebSocket endpoints already hold a persistent connection and are skipped.
    pub fn spawn_keepalive(&self, executor: &Runtime) {
        if !matches!(
            BuiltInConnectionString::from_str(self.endpoint()),
            Ok(BuiltInConnectionString::Http(_))
        ) {
            return;
        }
        let inner = Arc::downgrade(&self.inner);
        executor.spawn_task(async move {
            let mut ticker = tokio::time::interval(Self::KEEPALIVE_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(inner) = inner.upgrade() else { break };
                // The Base sequencer endpoint rejects eth_chainId at its proxy with a JSON-RPC
                // error, so no mempool node sees it. The transport reads error bodies in full,
                // which returns the connection to the pool either way.
                let result = tokio::time::timeout(
                    Self::KEEPALIVE_TIMEOUT,
                    inner.client.request::<_, U64>("eth_chainId", ()),
                )
                .await;
                match result {
                    Ok(Ok(_)) => trace!(target: "rpc::sequencer", "sequencer keepalive answered"),
                    Ok(Err(err)) if err.is_error_resp() => {
                        trace!(target: "rpc::sequencer", %err, "sequencer keepalive answered")
                    }
                    Ok(Err(err)) => {
                        SequencerMetrics::keepalive_failures().increment(1);
                        debug!(target: "rpc::sequencer", %err, "sequencer keepalive failed");
                    }
                    Err(_) => {
                        SequencerMetrics::keepalive_failures().increment(1);
                        debug!(target: "rpc::sequencer", "sequencer keepalive timed out");
                    }
                }
            }
        });
    }

    /// Forwards a transaction to the sequencer endpoint.
    pub async fn forward_raw_transaction(&self, tx: &[u8]) -> Result<B256, SequencerClientError> {
        let start = Instant::now();
        let rlp_hex = hex::encode_prefixed(tx);
        let tx_hash =
            self.request("eth_sendRawTransaction", (rlp_hex,)).await.inspect_err(|err| {
                warn!(
                    target: "rpc::eth",
                    %err,
                    "Failed to forward transaction to sequencer",
                );
            })?;
        SequencerMetrics::record_forward_latency(start.elapsed());
        Ok(tx_hash)
    }
}

/// Inner state of a [`SequencerClient`].
#[derive(Debug)]
pub struct SequencerClientInner {
    /// The endpoint of the sequencer
    pub sequencer_endpoint: String,
    /// The RPC client
    pub client: Client,
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U64;

    use super::*;

    #[tokio::test]
    async fn test_http_body_str() {
        let client = SequencerClient::new("http://localhost:8545").await.unwrap();

        let request = client
            .client()
            .make_request("eth_getBlockByNumber", (U64::from(10),))
            .serialize()
            .unwrap()
            .take_request();
        let body = request.get();

        assert_eq!(
            body,
            r#"{"method":"eth_getBlockByNumber","params":["0xa"],"id":0,"jsonrpc":"2.0"}"#
        );
    }

    #[tokio::test]
    #[ignore = "Start if WS is reachable at ws://localhost:8546"]
    async fn test_ws_body_str() {
        let client = SequencerClient::new("ws://localhost:8546").await.unwrap();

        let request = client
            .client()
            .make_request("eth_getBlockByNumber", (U64::from(10),))
            .serialize()
            .unwrap()
            .take_request();
        let body = request.get();

        assert_eq!(
            body,
            r#"{"method":"eth_getBlockByNumber","params":["0xa"],"id":0,"jsonrpc":"2.0"}"#
        );
    }
}
