mod branch;
mod node;
mod path;

#[cfg(test)]
pub use self::node::test_utils as node_test_utils;
pub use self::{
    branch::Branch,
    node::{
        detect_complete_snapshots, InnerNode, InnerNodeMap, LeafNode, LeafNodeSet, RootNode,
        INNER_LAYER_COUNT,
    },
};

use crate::{
    block::BlockId,
    db,
    error::{Error, Result},
    ReplicaId,
};
use sqlx::Row;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

type SnapshotId = u32;

#[derive(Clone)]
pub struct Index {
    pub pool: db::Pool,
    pub this_replica_id: ReplicaId,
    branches: Arc<Mutex<Branches>>,
}

impl Index {
    pub async fn load(pool: db::Pool, this_replica_id: ReplicaId) -> Result<Self> {
        let mut tx = pool.begin().await?;

        let local = Branch::new(&mut tx, this_replica_id).await?;
        let remote = load_remote_branches(&mut tx, &this_replica_id).await?;

        tx.commit().await?;

        let branches = Branches { local, remote };

        Ok(Self {
            pool,
            this_replica_id,
            branches: Arc::new(Mutex::new(branches)),
        })
    }

    pub async fn remote_branch(&self, replica_id: &ReplicaId) -> Option<Branch> {
        self.branches.lock().await.remote.get(replica_id).cloned()
    }

    pub async fn local_branch(&self) -> Branch {
        self.branches.lock().await.local.clone()
    }
}

struct Branches {
    local: Branch,
    remote: HashMap<ReplicaId, Branch>,
}

/// Returns all replica ids we know of except ours.
async fn load_other_replica_ids(
    tx: &mut db::Transaction,
    this_replica_id: &ReplicaId,
) -> Result<Vec<ReplicaId>> {
    Ok(
        sqlx::query("SELECT DISTINCT replica_id FROM snapshot_root_nodes WHERE replica_id <> ?")
            .bind(this_replica_id)
            .map(|row| row.get(0))
            .fetch_all(tx)
            .await?,
    )
}

async fn load_remote_branches(
    tx: &mut db::Transaction,
    this_replica_id: &ReplicaId,
) -> Result<HashMap<ReplicaId, Branch>> {
    let ids = load_other_replica_ids(tx, this_replica_id).await?;
    let mut map = HashMap::new();

    for id in ids {
        let branch = Branch::new(tx, id).await?;
        map.insert(id, branch);
    }

    Ok(map)
}

/// Initializes the index. Creates the required database schema unless already exists.
pub async fn init(pool: &db::Pool) -> Result<(), Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS snapshot_root_nodes (
             snapshot_id INTEGER PRIMARY KEY,
             replica_id  BLOB NOT NULL,
             versions    BLOB NOT NULL,

             -- Hash of the children
             hash        BLOB NOT NULL,

             -- Is this snapshot completely downloaded?
             is_complete INTEGER NOT NULL,

             UNIQUE(replica_id, hash)
         );

         CREATE TABLE IF NOT EXISTS snapshot_inner_nodes (
             -- Parent's `hash`
             parent      BLOB NOT NULL,

             -- Index of this node within its siblings
             bucket      INTEGER NOT NULL,

             -- Hash of the children
             hash        BLOB NOT NULL,

             -- Is this subree completely downloaded?
             is_complete INTEGER NOT NULL,

             UNIQUE(parent, bucket)
         );

         CREATE TABLE IF NOT EXISTS snapshot_leaf_nodes (
             -- Parent's `hash`
             parent      BLOB NOT NULL,
             locator     BLOB NOT NULL,
             block_id    BLOB NOT NULL
         );

         -- Prevents creating multiple inner nodes with the same parent and bucket but different
         -- hash.
         CREATE TRIGGER IF NOT EXISTS snapshot_inner_nodes_conflict_check
         BEFORE INSERT ON snapshot_inner_nodes
         WHEN EXISTS(
             SELECT 0
             FROM snapshot_inner_nodes
             WHERE parent = new.parent
               AND bucket = new.bucket
               AND hash <> new.hash
         )
         BEGIN
             SELECT RAISE (ABORT, 'inner node conflict');
         END;",
    )
    .execute(pool)
    .await
    .map_err(Error::CreateDbSchema)?;

    Ok(())
}

/// Removes the block if it's orphaned (not referenced by any branch), otherwise does nothing.
/// Returns whether the block was removed.
pub async fn remove_orphaned_block(tx: &mut db::Transaction, id: &BlockId) -> Result<bool> {
    let result = sqlx::query(
        "DELETE FROM blocks
         WHERE id = ? AND (SELECT 0 FROM snapshot_leaf_nodes WHERE block_id = id) IS NULL",
    )
    .bind(id)
    .execute(tx)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        block::{self, BLOCK_SIZE},
        crypto::{AuthTag, Cryptor, Hashable},
        locator::Locator,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_block() {
        let pool = db::Pool::connect(":memory:").await.unwrap();
        init(&pool).await.unwrap();
        block::init(&pool).await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        let cryptor = Cryptor::Null;

        let branch0 = Branch::new(&mut tx, ReplicaId::random()).await.unwrap();
        let branch1 = Branch::new(&mut tx, ReplicaId::random()).await.unwrap();

        let block_id = BlockId::random();
        let buffer = vec![0; BLOCK_SIZE];

        block::write(&mut tx, &block_id, &buffer, &AuthTag::default())
            .await
            .unwrap();

        let locator0 = Locator::Head(rand::random::<u64>().hash(), 0);
        let locator0 = locator0.encode(&cryptor);
        branch0.insert(&mut tx, &block_id, &locator0).await.unwrap();

        let locator1 = Locator::Head(rand::random::<u64>().hash(), 0);
        let locator1 = locator1.encode(&cryptor);
        branch1.insert(&mut tx, &block_id, &locator1).await.unwrap();

        assert!(!remove_orphaned_block(&mut tx, &block_id).await.unwrap());
        assert!(block::exists(&mut tx, &block_id).await.unwrap());

        branch0.remove(&mut tx, &locator0).await.unwrap();

        assert!(!remove_orphaned_block(&mut tx, &block_id).await.unwrap());
        assert!(block::exists(&mut tx, &block_id).await.unwrap());

        branch1.remove(&mut tx, &locator1).await.unwrap();

        assert!(remove_orphaned_block(&mut tx, &block_id).await.unwrap());
        assert!(!block::exists(&mut tx, &block_id).await.unwrap(),);
    }
}