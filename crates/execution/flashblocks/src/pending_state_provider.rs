//! Pending state overlay backed by the flashblocks bundle state.
//!
//! reth resolves `BlockId::pending()` by walking the published block's parent chain down to a
//! block it holds, then layering the in-memory diffs on top. Flashblocks is a pre-confirmation
//! stream, so it routinely runs ahead of canonical ingestion and the blocks in between exist
//! only here — the chain is broken and the walk fails with `block not found: hash`.
//!
//! The bundle does not need that chain. It is computed against `earliest - 1`, which is always a
//! real historical block, and it already covers every change from there up to the pending block.
//! Anchoring on it directly makes the overlay independent of how far ahead flashblocks runs.

use std::sync::{Arc, OnceLock};

use alloy_primitives::{Address, B256, BlockNumber, Bytes, StorageKey, StorageValue};
use base_execution_rpc::{PendingStateSnapshot, PendingStateSource};
use reth_primitives_traits::{Account, Bytecode};
use reth_storage_api::{
    AccountReader, BlockHashReader, BytecodeReader, HashedPostStateProvider, StateProofProvider,
    StateProvider, StateProviderBox, StateRootProvider, StorageRootProvider,
};
use reth_storage_errors::provider::ProviderResult;
use reth_trie::{
    AccountProof, ExecutionWitnessMode, HashedPostState, HashedStorage, KeccakKeyHasher,
    MultiProof, MultiProofTargets, StorageMultiProof, TrieInput, updates::TrieUpdates,
};
use revm::database::BundleState;

use crate::{FlashblocksAPI, FlashblocksState, PendingBlocks};

/// Snapshot handed to the RPC layer so the anchor and the diff always come from the same
/// `PendingBlocks`; reading them separately could straddle a flashblock update.
pub struct FlashblocksPendingSnapshot {
    pending: Arc<PendingBlocks>,
}

impl std::fmt::Debug for FlashblocksPendingSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlashblocksPendingSnapshot")
            .field("anchor", &self.pending.parent_hash())
            .field("pending_block", &self.pending.latest_block_number())
            .finish()
    }
}

impl PendingStateSnapshot for FlashblocksPendingSnapshot {
    fn anchor_hash(&self) -> B256 {
        // Parent of the earliest pending block: the block the bundle was computed against.
        self.pending.parent_hash()
    }

    fn overlay(self: Box<Self>, historical: StateProviderBox) -> StateProviderBox {
        Box::new(FlashblocksPendingStateProvider {
            historical,
            bundle: self.pending.get_bundle_state(),
            hashed_state: OnceLock::new(),
        })
    }
}

impl PendingStateSource for FlashblocksState {
    fn snapshot(&self) -> Option<Box<dyn PendingStateSnapshot>> {
        let pending = self.get_pending_blocks().clone()?;
        Some(Box::new(FlashblocksPendingSnapshot { pending }))
    }
}

/// `earliest - 1` state with the flashblocks bundle layered on top.
pub struct FlashblocksPendingStateProvider {
    historical: StateProviderBox,
    bundle: Arc<BundleState>,
    hashed_state: OnceLock<HashedPostState>,
}

impl std::fmt::Debug for FlashblocksPendingStateProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlashblocksPendingStateProvider")
            .field("accounts", &self.bundle.state.len())
            .finish_non_exhaustive()
    }
}

impl FlashblocksPendingStateProvider {
    /// Hashed form of the bundle. `eth_call` never reaches this; only the root and proof methods
    /// do, so the hashing cost is paid solely by callers that actually ask for them.
    fn hashed_state(&self) -> &HashedPostState {
        self.hashed_state.get_or_init(|| {
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(self.bundle.state.iter())
        })
    }

    /// The pending diff has to sit underneath the caller's own overlay, so it is prepended rather
    /// than extended onto.
    fn prepend_pending(&self, input: &mut TrieInput) {
        input.prepend_cached(TrieUpdates::default(), self.hashed_state().clone());
    }

    fn merged_hashed_storage(&self, address: Address, storage: HashedStorage) -> HashedStorage {
        let mut hashed = self
            .hashed_state()
            .storages
            .get(&alloy_primitives::keccak256(address))
            .cloned()
            .unwrap_or_default();
        hashed.extend(&storage);
        hashed
    }
}

impl AccountReader for FlashblocksPendingStateProvider {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        // Present in the bundle means the account was touched between `earliest` and the pending
        // block, so the bundle holds its final value. `info: None` means it is gone.
        if let Some(account) = self.bundle.account(address) {
            return Ok(account.info.as_ref().map(|info| Account {
                nonce: info.nonce,
                balance: info.balance,
                bytecode_hash: (!info.is_empty_code_hash()).then_some(info.code_hash),
            }));
        }

        self.historical.basic_account(address)
    }
}

impl BlockHashReader for FlashblocksPendingStateProvider {
    fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        self.historical.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.historical.canonical_hashes_range(start, end)
    }
}

impl StateProvider for FlashblocksPendingStateProvider {
    fn storage(
        &self,
        address: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        // `storage_slot` reports zero for untracked slots of an account whose storage is fully
        // known (created or destroyed), and `None` only when the bundle cannot answer.
        if let Some(value) = self.bundle.storage(&address, storage_key.into()) {
            return Ok(Some(value));
        }

        self.historical.storage(address, storage_key)
    }
}

impl BytecodeReader for FlashblocksPendingStateProvider {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        if let Some(bytecode) = self.bundle.bytecode(code_hash) {
            return Ok(Some(Bytecode(bytecode)));
        }

        self.historical.bytecode_by_hash(code_hash)
    }
}

impl StateRootProvider for FlashblocksPendingStateProvider {
    fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
        self.state_root_from_nodes(TrieInput::from_state(state))
    }

    fn state_root_from_nodes(&self, mut input: TrieInput) -> ProviderResult<B256> {
        self.prepend_pending(&mut input);
        self.historical.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.state_root_from_nodes_with_updates(TrieInput::from_state(state))
    }

    fn state_root_from_nodes_with_updates(
        &self,
        mut input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.prepend_pending(&mut input);
        self.historical.state_root_from_nodes_with_updates(input)
    }
}

impl StorageRootProvider for FlashblocksPendingStateProvider {
    fn storage_root(&self, address: Address, storage: HashedStorage) -> ProviderResult<B256> {
        let merged = self.merged_hashed_storage(address, storage);
        self.historical.storage_root(address, merged)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        let merged = self.merged_hashed_storage(address, storage);
        self.historical.storage_proof(address, slot, merged)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        let merged = self.merged_hashed_storage(address, storage);
        self.historical.storage_multiproof(address, slots, merged)
    }
}

impl StateProofProvider for FlashblocksPendingStateProvider {
    fn proof(
        &self,
        mut input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        self.prepend_pending(&mut input);
        self.historical.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        mut input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        self.prepend_pending(&mut input);
        self.historical.multiproof(input, targets)
    }

    fn witness(
        &self,
        mut input: TrieInput,
        target: HashedPostState,
        mode: ExecutionWitnessMode,
    ) -> ProviderResult<Vec<Bytes>> {
        self.prepend_pending(&mut input);
        self.historical.witness(input, target, mode)
    }
}

impl HashedPostStateProvider for FlashblocksPendingStateProvider {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        self.historical.hashed_post_state(bundle_state)
    }
}
