//! Merges flashblocks from several upstream connections into one in-order stream.

use alloy_rpc_types_engine::PayloadId;
use base_common_flashblocks::Flashblock;

/// What [`FlashblockSequenceGate::admit`] decided for an incoming flashblock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceGateDecision {
    /// The flashblock continues the forwarded sequence and should reach the processor.
    Forward,
    /// The flashblock is at or behind the forwarded position: a slower upstream's copy.
    Duplicate,
    /// The flashblock skips past the next expected one.
    OutOfSequence,
}

/// Forwarded position of a [`FlashblockSequenceGate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceGatePosition {
    /// Block number of the last forwarded flashblock.
    pub block_number: u64,
    /// Payload id of the last forwarded flashblock.
    pub payload_id: PayloadId,
    /// Index of the last forwarded flashblock.
    pub index: u64,
    /// Payload ids forwarded for `block_number`, so a late copy of an earlier payload's
    /// index 0 is not mistaken for another builder restart.
    pub payload_ids: Vec<PayloadId>,
}

/// Lets through only what one in-order upstream connection would deliver.
///
/// The processor clears all pending state on any flashblock that does not extend the current
/// one, so a merge of several connections must never pass on a late copy or a flashblock that
/// skips ahead. When every upstream misses a flashblock, the gate waits for the next block's
/// index 0, the same point a single connection recovers at.
#[derive(Debug, Clone, Default)]
pub struct FlashblockSequenceGate {
    /// Last forwarded position, `None` until the first flashblock arrives.
    position: Option<SequenceGatePosition>,
}

impl FlashblockSequenceGate {
    /// Decides whether `flashblock` is forwarded, and advances the gate if it is.
    pub fn admit(&mut self, flashblock: &Flashblock) -> SequenceGateDecision {
        let block_number = flashblock.metadata.block_number;
        let index = flashblock.index;
        let payload_id = flashblock.payload_id;

        let Some(position) = self.position.as_mut() else {
            // A fresh connection starts wherever the stream is, so the first arrival is
            // forwarded as is, whatever its index.
            self.position = Some(SequenceGatePosition {
                block_number,
                payload_id,
                index,
                payload_ids: vec![payload_id],
            });
            return SequenceGateDecision::Forward;
        };

        if block_number > position.block_number {
            if index != 0 {
                return SequenceGateDecision::OutOfSequence;
            }
            *position = SequenceGatePosition {
                block_number,
                payload_id,
                index,
                payload_ids: vec![payload_id],
            };
            return SequenceGateDecision::Forward;
        }

        if block_number < position.block_number {
            return SequenceGateDecision::Duplicate;
        }

        if payload_id == position.payload_id {
            if index == position.index + 1 {
                position.index = index;
                return SequenceGateDecision::Forward;
            }
            if index <= position.index {
                return SequenceGateDecision::Duplicate;
            }
            return SequenceGateDecision::OutOfSequence;
        }

        if position.payload_ids.contains(&payload_id) {
            return SequenceGateDecision::Duplicate;
        }
        if index != 0 {
            return SequenceGateDecision::OutOfSequence;
        }
        // Same block, unseen payload id, index 0: the builder restarted the block. One
        // connection would pass this on too, so the processor keeps handling it as today.
        position.payload_id = payload_id;
        position.index = 0;
        position.payload_ids.push(payload_id);
        SequenceGateDecision::Forward
    }
}

#[cfg(test)]
mod tests {
    use base_common_flashblocks::{ExecutionPayloadFlashblockDeltaV1, Metadata};

    use super::*;

    fn flashblock(block_number: u64, payload: u8, index: u64) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([payload; 8]),
            index,
            base: None,
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata::new(block_number),
        }
    }

    // Two upstreams interleaved, one of them a flashblock behind: every position reaches the
    // processor exactly once and in order, and nothing late or ahead slips through, because
    // either would make the processor wipe its pending state.
    #[test]
    fn interleaved_upstreams_forward_each_flashblock_once_in_order() {
        let mut gate = FlashblockSequenceGate::default();
        let arrivals = [
            flashblock(10, 1, 3),
            flashblock(10, 1, 4),
            flashblock(10, 1, 3),
            flashblock(10, 1, 6),
            flashblock(10, 1, 5),
            flashblock(10, 1, 4),
            flashblock(10, 1, 6),
            flashblock(11, 2, 1),
            flashblock(11, 2, 0),
            flashblock(10, 1, 7),
            flashblock(11, 2, 1),
            flashblock(11, 2, 0),
        ];
        let forwarded: Vec<_> = arrivals
            .iter()
            .filter(|fb| gate.admit(fb) == SequenceGateDecision::Forward)
            .map(|fb| (fb.metadata.block_number, fb.index))
            .collect();

        assert_eq!(forwarded, [(10, 3), (10, 4), (10, 5), (10, 6), (11, 0), (11, 1)]);
    }

    // A builder restart re-emits index 0 for the same block under a new payload id. It must be
    // forwarded once, and a slower upstream's copy of the old payload must not be taken for a
    // second restart.
    #[test]
    fn builder_restart_is_forwarded_once() {
        let mut gate = FlashblockSequenceGate::default();
        let arrivals = [
            (flashblock(10, 1, 0), SequenceGateDecision::Forward),
            (flashblock(10, 1, 1), SequenceGateDecision::Forward),
            (flashblock(10, 2, 0), SequenceGateDecision::Forward),
            (flashblock(10, 1, 2), SequenceGateDecision::Duplicate),
            (flashblock(10, 1, 0), SequenceGateDecision::Duplicate),
            (flashblock(10, 2, 0), SequenceGateDecision::Duplicate),
            (flashblock(10, 3, 1), SequenceGateDecision::OutOfSequence),
            (flashblock(10, 2, 1), SequenceGateDecision::Forward),
        ];
        for (fb, expected) in arrivals {
            assert_eq!(gate.admit(&fb), expected, "fb {}:{}", fb.metadata.block_number, fb.index);
        }
    }
}
