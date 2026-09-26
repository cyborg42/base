//! Loads Base pending block for a RPC response.

use alloy_eips::BlockNumberOrTag;
use reth_rpc_eth_api::{
    FromEvmError, RpcConvert, RpcNodeCore, RpcNodeCoreExt,
    helpers::{LoadPendingBlock, SpawnBlocking, pending_block::PendingEnvBuilder},
};
use reth_rpc_eth_types::{
    EthApiError, PendingBlock, block::BlockAndReceipts, builder::config::PendingBlockKind,
    error::FromEthApiError,
};
use reth_storage_api::{BlockReaderIdExt, StateProviderBox, StateProviderFactory};

use crate::{BaseEthApi, BaseEthApiError};

impl<N, Rpc> LoadPendingBlock for BaseEthApi<N, Rpc>
where
    N: RpcNodeCore,
    BaseEthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<Primitives = N::Primitives, Error = BaseEthApiError>,
{
    #[inline]
    fn pending_block(&self) -> &tokio::sync::Mutex<Option<PendingBlock<N::Primitives>>> {
        self.inner.eth_api.pending_block()
    }

    #[inline]
    fn pending_env_builder(&self) -> &dyn PendingEnvBuilder<Self::Evm> {
        self.inner.eth_api.pending_env_builder()
    }

    #[inline]
    fn pending_block_kind(&self) -> PendingBlockKind {
        self.inner.eth_api.pending_block_kind()
    }

    /// Returns the pending state overlay supplied by the configured source, if any.
    ///
    /// This is consulted by `LoadState::state_at_block_id` before the provider, which is what
    /// keeps pending resolution off reth's parent-chain walk. A pre-confirmation source runs
    /// ahead of canonical ingestion, so that walk hits blocks the node does not hold yet and
    /// fails with `block not found: hash`. Anchoring on the block the source computed its diff
    /// against avoids the chain entirely.
    ///
    /// There is no mem-pool built pending block on this stack, so without a source there is
    /// nothing to return and callers fall back to the provider.
    async fn local_pending_state(&self) -> Result<Option<StateProviderBox>, Self::Error>
    where
        Self: SpawnBlocking,
    {
        let Some(source) = self.pending_state_source() else {
            return Ok(None);
        };
        let Some(snapshot) = source.snapshot() else {
            return Ok(None);
        };
        // The caller discards this error and falls through to the provider, so without a log the
        // overlay could stop being used and nothing would say so.
        let historical = match self.provider().history_by_block_hash(snapshot.anchor_hash()) {
            Ok(historical) => historical,
            Err(err) => {
                tracing::warn!(
                    target: "rpc::eth",
                    %err,
                    anchor = %snapshot.anchor_hash(),
                    "pending state anchor is not available, falling back to provider"
                );
                return Ok(None);
            }
        };
        Ok(Some(snapshot.overlay(historical)))
    }

    /// Returns the locally built pending block
    async fn local_pending_block(
        &self,
    ) -> Result<Option<BlockAndReceipts<Self::Primitives>>, Self::Error> {
        // See: <https://github.com/ethereum-optimism/op-geth/blob/f2e69450c6eec9c35d56af91389a1c47737206ca/miner/worker.go#L367-L375>
        let latest = self
            .provider()
            .latest_header()?
            .ok_or(EthApiError::HeaderNotFound(BlockNumberOrTag::Latest.into()))?;

        let latest = self
            .cache()
            .get_block_and_receipts(latest.hash())
            .await
            .map_err(Self::Error::from_eth_err)?
            .map(|(block, receipts)| BlockAndReceipts { block, receipts });
        Ok(latest)
    }
}
