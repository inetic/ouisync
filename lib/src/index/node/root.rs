use super::{
    super::{proof::Proof, SnapshotId},
    inner::{InnerNode, EMPTY_INNER_HASH},
    summary::{MultiBlockPresence, NodeState, Summary},
};
use crate::{
    crypto::{
        sign::{Keypair, PublicKey},
        Hash,
    },
    db,
    debug::DebugPrinter,
    error::{Error, Result},
    version_vector::VersionVector,
    versioned::Versioned,
};
use futures_util::{Stream, StreamExt, TryStreamExt};
use sqlx::Row;
use std::cmp::Ordering;

const EMPTY_SNAPSHOT_ID: SnapshotId = 0;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct RootNode {
    pub snapshot_id: SnapshotId,
    pub proof: Proof,
    pub summary: Summary,
}

impl RootNode {
    /// Creates a root node with no children without storing it in the database.
    pub fn empty(writer_id: PublicKey, write_keys: &Keypair) -> Self {
        let proof = Proof::new(
            writer_id,
            VersionVector::new(),
            *EMPTY_INNER_HASH,
            write_keys,
        );

        Self {
            snapshot_id: EMPTY_SNAPSHOT_ID,
            proof,
            summary: Summary {
                state: NodeState::Approved,
                block_presence: MultiBlockPresence::Full,
            },
        }
    }

    /// Creates a root node with the specified proof.
    ///
    /// The version vector must be greater than the version vector of any currently existing root
    /// node in the same branch, otherwise no node is created and an error is returned.
    pub async fn create(
        tx: &mut db::WriteTransaction,
        proof: Proof,
        summary: Summary,
    ) -> Result<Self> {
        // Check that the root node to be created is newer than the latest existing root node in
        // the same branch.
        let old_vv: VersionVector = sqlx::query(
            "SELECT versions
             FROM snapshot_root_nodes
             WHERE snapshot_id = (
                 SELECT MAX(snapshot_id)
                 FROM snapshot_root_nodes
                 WHERE writer_id = ?
             )",
        )
        .bind(&proof.writer_id)
        .map(|row| row.get(0))
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or_else(VersionVector::new);

        match proof.version_vector.partial_cmp(&old_vv) {
            Some(Ordering::Greater) => (),
            Some(Ordering::Equal | Ordering::Less) => {
                tracing::warn!(
                    ?old_vv,
                    new_vv = ?proof.version_vector,
                    "attempt to create outdated root node"
                );
                return Err(Error::EntryExists);
            }
            None => {
                tracing::warn!("attempt to create concurrent root node in the same branch");
                return Err(Error::OperationNotSupported);
            }
        }

        let snapshot_id = sqlx::query(
            "INSERT INTO snapshot_root_nodes (
                 writer_id,
                 versions,
                 hash,
                 signature,
                 state,
                 block_presence
             )
             VALUES (?, ?, ?, ?, ?, ?)
             RETURNING snapshot_id",
        )
        .bind(&proof.writer_id)
        .bind(&proof.version_vector)
        .bind(&proof.hash)
        .bind(&proof.signature)
        .bind(summary.state)
        .bind(&summary.block_presence)
        .map(|row| row.get(0))
        .fetch_one(tx)
        .await?;

        Ok(Self {
            snapshot_id,
            proof,
            summary,
        })
    }

    /// Returns the latest approved root node of the specified writer.
    pub async fn load_latest_approved_by_writer(
        conn: &mut db::Connection,
        writer_id: PublicKey,
    ) -> Result<Self> {
        sqlx::query(
            "SELECT
                 snapshot_id,
                 versions,
                 hash,
                 signature,
                 block_presence
             FROM
                 snapshot_root_nodes
             WHERE
                 snapshot_id = (
                     SELECT MAX(snapshot_id)
                     FROM snapshot_root_nodes
                     WHERE writer_id = ? AND state = ?
                 )
            ",
        )
        .bind(&writer_id)
        .bind(NodeState::Approved)
        .fetch_optional(conn)
        .await?
        .map(|row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(writer_id, row.get(1), row.get(2), row.get(3)),
            summary: Summary {
                state: NodeState::Approved,
                block_presence: row.get(4),
            },
        })
        .ok_or(Error::EntryNotFound)
    }

    /// Return the latest approved root nodes of all known writers.
    pub fn load_all_latest_approved(
        conn: &mut db::Connection,
    ) -> impl Stream<Item = Result<Self>> + '_ {
        sqlx::query(
            "SELECT
                 snapshot_id,
                 writer_id,
                 versions,
                 hash,
                 signature,
                 block_presence
             FROM
                 snapshot_root_nodes
             WHERE
                 snapshot_id IN (
                     SELECT MAX(snapshot_id)
                     FROM snapshot_root_nodes
                     WHERE state = ?
                     GROUP BY writer_id
                 )",
        )
        .bind(NodeState::Approved)
        .fetch(conn)
        .map_ok(|row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(row.get(1), row.get(2), row.get(3), row.get(4)),
            summary: Summary {
                state: NodeState::Approved,
                block_presence: row.get(5),
            },
        })
        .err_into()
    }

    /// Return the latest root nodes of all known writers.
    pub fn load_all_latest(conn: &mut db::Connection) -> impl Stream<Item = Result<Self>> + '_ {
        sqlx::query(
            "SELECT
                 snapshot_id,
                 writer_id,
                 versions,
                 hash,
                 signature,
                 state,
                 block_presence
             FROM
                 snapshot_root_nodes
             WHERE
                 snapshot_id IN (
                     SELECT MAX(snapshot_id)
                     FROM snapshot_root_nodes
                     GROUP BY writer_id
                 )",
        )
        .fetch(conn)
        .map_ok(|row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(row.get(1), row.get(2), row.get(3), row.get(4)),
            summary: Summary {
                state: row.get(5),
                block_presence: row.get(6),
            },
        })
        .err_into()
    }

    /// Returns a stream of all root nodes corresponding to the specified writer ordered from the
    /// most recent to the least recent.
    #[cfg(test)]
    pub fn load_all_by_writer(
        conn: &mut db::Connection,
        writer_id: PublicKey,
    ) -> impl Stream<Item = Result<Self>> + '_ {
        sqlx::query(
            "SELECT
                 snapshot_id,
                 versions,
                 hash,
                 signature,
                 state,
                 block_presence
             FROM snapshot_root_nodes
             WHERE writer_id = ?
             ORDER BY snapshot_id DESC",
        )
        .bind(writer_id.as_ref().to_owned()) // needed to satisfy the borrow checker.
        .fetch(conn)
        .map_ok(move |row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(writer_id, row.get(1), row.get(2), row.get(3)),
            summary: Summary {
                state: row.get(4),
                block_presence: row.get(5),
            },
        })
        .err_into()
    }

    /// Returns the latest root node of the specified writer or `None` if no snapshot of that
    /// writer exists.
    #[cfg(test)]
    pub async fn load_latest_by_writer(
        conn: &mut db::Connection,
        writer_id: PublicKey,
    ) -> Result<Option<Self>> {
        Self::load_all_by_writer(conn, writer_id).try_next().await
    }

    /// Returns all nodes with the specified hash
    pub fn load_all_by_hash<'a>(
        conn: &'a mut db::Connection,
        hash: &'a Hash,
    ) -> impl Stream<Item = Result<Self>> + 'a {
        sqlx::query(
            "SELECT
                 snapshot_id,
                 writer_id,
                 versions,
                 signature,
                 state,
                 block_presence
             FROM snapshot_root_nodes
             WHERE hash = ?",
        )
        .bind(hash)
        .fetch(conn)
        .map_ok(move |row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(row.get(1), row.get(2), *hash, row.get(3)),
            summary: Summary {
                state: row.get(4),
                block_presence: row.get(5),
            },
        })
        .err_into()
    }

    /// Returns the writer ids of the nodes with the specified hash.
    pub fn load_writer_ids<'a>(
        conn: &'a mut db::Connection,
        hash: &'a Hash,
    ) -> impl Stream<Item = Result<PublicKey>> + 'a {
        sqlx::query("SELECT DISTINCT writer_id FROM snapshot_root_nodes WHERE hash = ?")
            .bind(hash)
            .fetch(conn)
            .map_ok(|row| row.get(0))
            .err_into()
    }

    /// Load the previous approved root node of the same writer.
    pub async fn load_prev(&self, conn: &mut db::Connection) -> Result<Option<Self>> {
        sqlx::query(
            "SELECT
                snapshot_id,
                versions,
                hash,
                signature,
                block_presence
             FROM snapshot_root_nodes
             WHERE writer_id = ? AND state = ? AND snapshot_id < ?
             ORDER BY snapshot_id DESC
             LIMIT 1",
        )
        .bind(&self.proof.writer_id)
        .bind(NodeState::Approved)
        .bind(self.snapshot_id)
        .fetch(conn)
        .map_ok(|row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(self.proof.writer_id, row.get(1), row.get(2), row.get(3)),
            summary: Summary {
                state: NodeState::Approved,
                block_presence: row.get(4),
            },
        })
        .err_into()
        .try_next()
        .await
    }

    /// Reload this root node from the db.
    #[cfg(test)]
    pub async fn reload(&mut self, conn: &mut db::Connection) -> Result<()> {
        let row = sqlx::query(
            "SELECT state, block_presence
             FROM snapshot_root_nodes
             WHERE snapshot_id = ?",
        )
        .bind(self.snapshot_id)
        .fetch_one(conn)
        .await?;

        self.summary.state = row.get(0);
        self.summary.block_presence = row.get(1);

        Ok(())
    }

    /// Update the summaries of all nodes with the specified hash.
    pub async fn update_summaries(tx: &mut db::WriteTransaction, hash: &Hash) -> Result<NodeState> {
        let summary = InnerNode::compute_summary(tx, hash).await?;

        let state = sqlx::query(
            "UPDATE snapshot_root_nodes
             SET block_presence = ?,
                 state = CASE state WHEN ? THEN ? ELSE state END
             WHERE hash = ?
             RETURNING state
             ",
        )
        .bind(&summary.block_presence)
        .bind(NodeState::Incomplete)
        .bind(summary.state)
        .bind(hash)
        .fetch_optional(tx)
        .await?
        .map(|row| row.get(0))
        .unwrap_or(NodeState::Incomplete);

        Ok(state)
    }

    /// Approve the nodes with the specified hash.
    pub async fn approve(tx: &mut db::WriteTransaction, hash: &Hash) -> Result<()> {
        Self::set_state(tx, hash, NodeState::Approved).await
    }

    /// Reject the nodes with the specified hash.
    pub async fn reject(tx: &mut db::WriteTransaction, hash: &Hash) -> Result<()> {
        Self::set_state(tx, hash, NodeState::Rejected).await
    }

    async fn set_state(tx: &mut db::WriteTransaction, hash: &Hash, state: NodeState) -> Result<()> {
        sqlx::query("UPDATE snapshot_root_nodes SET state = ? WHERE hash = ?")
            .bind(state)
            .bind(hash)
            .execute(tx)
            .await?;
        Ok(())
    }

    /// Removes this node including its snapshot.
    pub async fn remove_recursively(&self, tx: &mut db::WriteTransaction) -> Result<()> {
        // This uses db triggers to delete the whole snapshot.
        sqlx::query("DELETE FROM snapshot_root_nodes WHERE snapshot_id = ?")
            .bind(self.snapshot_id)
            .execute(tx)
            .await?;

        Ok(())
    }

    /// Removes all root nodes, including their snapshots, that are older than this node and are
    /// on the same branch.
    pub async fn remove_recursively_all_older(&self, tx: &mut db::WriteTransaction) -> Result<()> {
        // This uses db triggers to delete the whole snapshot.
        sqlx::query("DELETE FROM snapshot_root_nodes WHERE snapshot_id < ? AND writer_id = ?")
            .bind(self.snapshot_id)
            .bind(&self.proof.writer_id)
            .execute(tx)
            .await?;

        Ok(())
    }

    /// Removes all root nodes, including their snapshots, that are older than this node and are
    /// on the same branch and are not complete.
    pub async fn remove_recursively_all_older_incomplete(
        &self,
        tx: &mut db::WriteTransaction,
    ) -> Result<()> {
        // This uses db triggers to delete the whole snapshot.
        sqlx::query(
            "DELETE FROM snapshot_root_nodes
             WHERE snapshot_id < ? AND writer_id = ? AND state IN (?, ?)",
        )
        .bind(self.snapshot_id)
        .bind(&self.proof.writer_id)
        .bind(NodeState::Incomplete)
        .bind(NodeState::Rejected)
        .execute(tx)
        .await?;

        Ok(())
    }

    /// Does this node exist in the db?
    pub async fn exists(&self, conn: &mut db::Connection) -> Result<bool> {
        Ok(
            sqlx::query("SELECT 0 FROM snapshot_root_nodes WHERE snapshot_id = ?")
                .bind(self.snapshot_id)
                .fetch_optional(conn)
                .await?
                .is_some(),
        )
    }

    pub async fn debug_print(conn: &mut db::Connection, printer: DebugPrinter) {
        let mut roots = sqlx::query(
            "SELECT
                 snapshot_id,
                 versions,
                 hash,
                 signature,
                 state,
                 block_presence,
                 writer_id
             FROM snapshot_root_nodes
             ORDER BY snapshot_id DESC",
        )
        .fetch(conn)
        .map_ok(move |row| Self {
            snapshot_id: row.get(0),
            proof: Proof::new_unchecked(row.get(6), row.get(1), row.get(2), row.get(3)),
            summary: Summary {
                state: row.get(4),
                block_presence: row.get(5),
            },
        });

        while let Some(root_node) = roots.next().await {
            match root_node {
                Ok(root_node) => {
                    printer.debug(&format_args!(
                        "RootNode: snapshot_id:{:?}, writer_id:{:?}, vv:{:?}, state:{:?}",
                        root_node.snapshot_id,
                        root_node.proof.writer_id,
                        root_node.proof.version_vector,
                        root_node.summary.state
                    ));
                }
                Err(err) => {
                    printer.debug(&format_args!("RootNode: error: {:?}", err));
                }
            }
        }
    }
}

impl Versioned for RootNode {
    fn version_vector(&self) -> &VersionVector {
        &self.proof.version_vector
    }
}
