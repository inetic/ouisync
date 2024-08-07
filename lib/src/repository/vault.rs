//! Repository state and operations that don't require read or write access.

#[cfg(test)]
mod tests;

use super::{quota, LocalId, Metadata, RepositoryMonitor};
use crate::{
    block_tracker::{BlockPromise, BlockTracker, OfferState},
    crypto::CacheHash,
    db,
    debug::DebugPrinter,
    error::Result,
    event::{EventSender, Payload},
    protocol::{
        Block, BlockId, InnerNodes, LeafNodes, MultiBlockPresence, NodeState, ProofError,
        RepositoryId, StorageSize, UntrustedProof,
    },
    store::{self, InnerNodeReceiveStatus, LeafNodeReceiveStatus, RootNodeReceiveStatus, Store},
};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
use tracing::Instrument;

#[derive(Clone)]
pub(crate) struct Vault {
    repository_id: RepositoryId,
    store: Store,
    pub event_tx: EventSender,
    pub block_tracker: BlockTracker,
    pub local_id: LocalId,
    pub monitor: Arc<RepositoryMonitor>,
}

impl Vault {
    pub fn new(
        repository_id: RepositoryId,
        event_tx: EventSender,
        pool: db::Pool,
        monitor: RepositoryMonitor,
    ) -> Self {
        let store = Store::new(pool);

        Self {
            repository_id,
            store,
            event_tx,
            block_tracker: BlockTracker::new(),
            local_id: LocalId::new(),
            monitor: Arc::new(monitor),
        }
    }

    pub fn repository_id(&self) -> &RepositoryId {
        &self.repository_id
    }

    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// Receive `RootNode` from other replica and store it into the db. Returns whether the
    /// received node has any new information compared to all the nodes already stored locally.
    pub async fn receive_root_node(
        &self,
        proof: UntrustedProof,
        block_presence: MultiBlockPresence,
    ) -> Result<RootNodeReceiveStatus> {
        let proof = match proof.verify(self.repository_id()) {
            Ok(proof) => proof,
            Err(ProofError(proof)) => {
                tracing::trace!(branch_id = ?proof.writer_id, hash = ?proof.hash, "Invalid proof");
                return Ok(RootNodeReceiveStatus::Outdated);
            }
        };

        // Ignore branches with empty version vectors because they have no content yet.
        if proof.version_vector.is_empty() {
            return Ok(RootNodeReceiveStatus::Outdated);
        }

        let mut tx = self.store().begin_write().await?;
        let status = tx.receive_root_node(proof, block_presence).await?;
        tx.commit().await?;

        Ok(status)
    }

    /// Receive inner nodes from other replica and store them into the db.
    /// Returns hashes of those nodes that were more up to date than the locally stored ones.
    /// Also returns the receive status.
    pub async fn receive_inner_nodes(
        &self,
        nodes: CacheHash<InnerNodes>,
    ) -> Result<InnerNodeReceiveStatus> {
        let mut tx = self.store().begin_write().await?;
        let status = tx.receive_inner_nodes(nodes).await?;
        tx.commit().await?;

        Ok(status)
    }

    /// Receive leaf nodes from other replica and store them into the db.
    /// Returns the ids of the blocks that the remote replica has but the local one has not.
    /// Also returns the receive status.
    pub async fn receive_leaf_nodes(
        &self,
        nodes: CacheHash<LeafNodes>,
    ) -> Result<LeafNodeReceiveStatus> {
        let mut tx = self.store().begin_write().await?;
        let status = tx.receive_leaf_nodes(nodes).await?;

        tx.commit_and_then({
            let new_approved = status.new_approved.clone();
            let event_tx = self.event_tx.clone();

            move || {
                for branch_id in new_approved {
                    event_tx.send(Payload::BranchChanged(branch_id));
                }
            }
        })
        .await?;

        Ok(status)
    }

    /// Receive a block from other replica.
    pub async fn receive_block(&self, block: &Block, promise: Option<BlockPromise>) -> Result<()> {
        let block_id = block.id;
        let event_tx = self.event_tx.clone();

        let mut tx = self.store().begin_write().await?;
        match tx.receive_block(block).await {
            Ok(()) => (),
            Err(error) => {
                if matches!(error, store::Error::BlockNotReferenced) {
                    // We no longer need this block but we still need to un-track it.
                    if let Some(promise) = promise {
                        promise.complete();
                    }
                }

                return Err(error.into());
            }
        };

        tx.commit_and_then(move || {
            event_tx.send(Payload::BlockReceived(block_id));

            if let Some(promise) = promise {
                promise.complete();
            }
        })
        .await?;

        Ok(())
    }

    /// Returns the state (`Pending` or `Approved`) that the offer for the given block should be
    /// registetred with. If the block isn't referenced or isn't missing, returns `None`.
    pub async fn offer_state(&self, block_id: &BlockId) -> Result<Option<OfferState>> {
        let mut r = self.store().acquire_read().await?;

        if quota::get(r.db()).await?.is_some() {
            // If quota is set we need to check what node state the snapshots referencing the block
            // are in and derive the offer state from that.
            match r.load_root_node_state_of_missing(block_id).await? {
                NodeState::Incomplete | NodeState::Complete => Ok(Some(OfferState::Pending)),
                NodeState::Approved => Ok(Some(OfferState::Approved)),
                NodeState::Rejected => Ok(None),
            }
        } else if r.is_block_missing(block_id).await? {
            Ok(Some(OfferState::Approved))
        } else {
            Ok(None)
        }
    }

    pub fn metadata(&self) -> Metadata {
        Metadata::new(self.store().db().clone())
    }

    /// Total size of the stored data
    pub async fn size(&self) -> Result<StorageSize> {
        let mut conn = self.store().db().acquire().await?;

        // Note: for simplicity, we are currently counting only blocks (content + id + nonce)
        let count = db::decode_u64(
            sqlx::query("SELECT COUNT(*) FROM blocks")
                .fetch_one(&mut *conn)
                .await?
                .get(0),
        );

        Ok(StorageSize::from_blocks(count))
    }

    pub async fn set_quota(&self, quota: Option<StorageSize>) -> Result<()> {
        let mut tx = self.store().db().begin_write().await?;

        if let Some(quota) = quota {
            quota::set(&mut tx, quota.to_bytes()).await?
        } else {
            quota::remove(&mut tx).await?
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn quota(&self) -> Result<Option<StorageSize>> {
        let mut conn = self.store().db().acquire().await?;
        Ok(quota::get(&mut conn).await?)
    }

    pub async fn set_block_expiration(&self, duration: Option<Duration>) -> Result<()> {
        Ok(self
            .store
            .set_block_expiration(duration, self.block_tracker.clone())
            .instrument(self.monitor.span().clone())
            .await?)
    }

    pub async fn block_expiration(&self) -> Option<Duration> {
        self.store.block_expiration().await
    }

    pub async fn debug_print(&self, print: DebugPrinter) {
        self.store().debug_print_root_node(print).await
    }
}
