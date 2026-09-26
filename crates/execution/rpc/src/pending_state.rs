//! Hook for supplying pending state from outside this crate.
//!
//! reth resolves `BlockId::pending()` through the published block's parent chain. A
//! pre-confirmation source such as flashblocks runs ahead of canonical ingestion, so that chain
//! is often incomplete and the lookup fails outright. `LoadState::state_at_block_id` consults
//! `local_pending_state` before touching the provider, which lets the source anchor the state
//! wherever it actually computed it.

use alloy_primitives::B256;
use reth_storage_api::StateProviderBox;

/// Supplies the pending state overlay, if one is currently available.
pub trait PendingStateSource: Send + Sync + 'static {
    /// Take a consistent snapshot. Returns `None` when there is no pending state, in which case
    /// `pending` is equivalent to `latest`.
    fn snapshot(&self) -> Option<Box<dyn PendingStateSnapshot>>;
}

/// One snapshot of pending state. The anchor and the diff are read together so they cannot
/// straddle an update.
pub trait PendingStateSnapshot: Send + Sync {
    /// Hash of the block the diff was computed against. Must be a block the node holds.
    fn anchor_hash(&self) -> B256;

    /// Layer the diff on top of the historical state at [`Self::anchor_hash`].
    fn overlay(self: Box<Self>, historical: StateProviderBox) -> StateProviderBox;
}
