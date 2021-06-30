use super::{
    inner::INNER_LAYER_COUNT, missing_blocks::MissingBlocksSummary, test_utils::Snapshot, *,
};
use crate::{crypto::Hashable, db, test_utils, version_vector::VersionVector};
use assert_matches::assert_matches;
use futures_util::TryStreamExt;
use rand::{distributions::Standard, prelude::*};
use test_strategy::proptest;

#[tokio::test(flavor = "multi_thread")]
async fn create_new_root_node() {
    let pool = setup().await;

    let replica_id = rand::random();
    let hash = rand::random::<u64>().hash();

    let node0 = RootNode::create(
        &pool,
        &replica_id,
        VersionVector::new(),
        hash,
        MissingBlocksSummary::default(),
    )
    .await
    .unwrap();
    assert_eq!(node0.hash, hash);

    let node1 = RootNode::load_latest_or_create(&pool, &replica_id)
        .await
        .unwrap();
    assert_eq!(node1, node0);

    let nodes: Vec<_> = RootNode::load_all(&pool, &replica_id, 2)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0], node0);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_existing_root_node() {
    let pool = setup().await;

    let replica_id = rand::random();
    let hash = rand::random::<u64>().hash();

    let node0 = RootNode::create(
        &pool,
        &replica_id,
        VersionVector::new(),
        hash,
        MissingBlocksSummary::default(),
    )
    .await
    .unwrap();

    let node1 = RootNode::create(
        &pool,
        &replica_id,
        VersionVector::new(),
        hash,
        MissingBlocksSummary::default(),
    )
    .await
    .unwrap();
    assert_eq!(node0, node1);

    let nodes: Vec<_> = RootNode::load_all(&pool, &replica_id, 2)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0], node0);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_new_inner_node() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let hash = rand::random::<u64>().hash();
    let bucket = rand::random();

    let mut tx = pool.begin().await.unwrap();

    let node = InnerNode::new(hash);
    node.save(&mut tx, &parent, bucket).await.unwrap();

    let nodes = InnerNode::load_children(&mut tx, &parent).await.unwrap();

    assert_eq!(nodes.get(bucket), Some(&node));

    assert!((0..bucket).all(|b| nodes.get(b).is_none()));

    if bucket < u8::MAX {
        assert!((bucket + 1..=u8::MAX).all(|b| nodes.get(b).is_none()));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_existing_inner_node() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let hash = rand::random::<u64>().hash();
    let bucket = rand::random();

    let mut tx = pool.begin().await.unwrap();

    let node0 = InnerNode::new(hash);
    node0.save(&mut tx, &parent, bucket).await.unwrap();

    let node1 = InnerNode::new(hash);
    node1.save(&mut tx, &parent, bucket).await.unwrap();

    let nodes = InnerNode::load_children(&mut tx, &parent).await.unwrap();

    assert_eq!(nodes.get(bucket), Some(&node0));
    assert!((0..bucket).all(|b| nodes.get(b).is_none()));
    assert!((bucket + 1..=u8::MAX).all(|b| nodes.get(b).is_none()));
}

#[tokio::test(flavor = "multi_thread")]
async fn attempt_to_create_conflicting_inner_node() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let bucket = rand::random();

    let hash0 = rand::random::<u64>().hash();
    let hash1 = loop {
        let hash = rand::random::<u64>().hash();
        if hash != hash0 {
            break hash;
        }
    };

    let mut tx = pool.begin().await.unwrap();

    let node0 = InnerNode::new(hash0);
    node0.save(&mut tx, &parent, bucket).await.unwrap();

    let node1 = InnerNode::new(hash1);
    assert_matches!(node1.save(&mut tx, &parent, bucket).await, Err(_)); // TODO: match concrete error type
}

#[tokio::test(flavor = "multi_thread")]
async fn update_inner_node_to_complete() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let bucket = rand::random();
    let hash = rand::random::<u64>().hash();

    let node = InnerNode::new(hash);
    let mut tx = pool.begin().await.unwrap();
    node.save(&mut tx, &parent, bucket).await.unwrap();
    tx.commit().await.unwrap();

    let nodes = InnerNode::load_children(&pool, &parent).await.unwrap();
    assert!(!nodes.get(bucket).unwrap().is_complete);

    InnerNode::set_complete(&pool, &hash).await.unwrap();

    let nodes = InnerNode::load_children(&pool, &parent).await.unwrap();
    assert!(nodes.get(bucket).unwrap().is_complete);
}

#[tokio::test(flavor = "multi_thread")]
async fn save_new_present_leaf_node() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let encoded_locator = rand::random::<u64>().hash();
    let block_id = rand::random();

    let mut tx = pool.begin().await.unwrap();

    let node = LeafNode::present(encoded_locator, block_id);
    node.save(&mut tx, &parent).await.unwrap();

    let nodes = LeafNode::load_children(&mut tx, &parent).await.unwrap();
    assert_eq!(nodes.len(), 1);

    let node = nodes.get(&encoded_locator).unwrap();
    assert_eq!(node.locator(), &encoded_locator);
    assert_eq!(node.block_id, block_id);
    assert!(!node.is_missing);
}

#[tokio::test(flavor = "multi_thread")]
async fn save_new_missing_leaf_node() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let encoded_locator = rand::random::<u64>().hash();
    let block_id = rand::random();

    let mut tx = pool.begin().await.unwrap();

    let node = LeafNode::missing(encoded_locator, block_id);
    node.save(&mut tx, &parent).await.unwrap();

    let nodes = LeafNode::load_children(&mut tx, &parent).await.unwrap();
    assert_eq!(nodes.len(), 1);

    let node = nodes.get(&encoded_locator).unwrap();
    assert_eq!(node.locator(), &encoded_locator);
    assert_eq!(node.block_id, block_id);
    assert!(node.is_missing);
}

#[tokio::test(flavor = "multi_thread")]
async fn save_missing_leaf_node_over_existing_missing_one() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let encoded_locator = rand::random::<u64>().hash();
    let block_id = rand::random();

    let mut tx = pool.begin().await.unwrap();

    let node = LeafNode::missing(encoded_locator, block_id);
    node.save(&mut tx, &parent).await.unwrap();

    let node = LeafNode::missing(encoded_locator, block_id);
    node.save(&mut tx, &parent).await.unwrap();

    let nodes = LeafNode::load_children(&mut tx, &parent).await.unwrap();
    assert_eq!(nodes.len(), 1);

    let node = nodes.get(&encoded_locator).unwrap();
    assert_eq!(node.locator(), &encoded_locator);
    assert_eq!(node.block_id, block_id);
    assert!(node.is_missing);
}

#[tokio::test(flavor = "multi_thread")]
async fn save_missing_leaf_node_over_exists_present_one() {
    let pool = setup().await;

    let parent = rand::random::<u64>().hash();
    let encoded_locator = rand::random::<u64>().hash();
    let block_id = rand::random();

    let mut tx = pool.begin().await.unwrap();

    let node = LeafNode::present(encoded_locator, block_id);
    node.save(&mut tx, &parent).await.unwrap();

    let node = LeafNode::missing(encoded_locator, block_id);
    node.save(&mut tx, &parent).await.unwrap();

    let nodes = LeafNode::load_children(&mut tx, &parent).await.unwrap();
    assert_eq!(nodes.len(), 1);

    let node = nodes.get(&encoded_locator).unwrap();
    assert_eq!(node.locator(), &encoded_locator);
    assert_eq!(node.block_id, block_id);
    assert!(!node.is_missing);
}

#[proptest]
fn check_complete(
    #[strategy(0usize..=32)] leaf_count: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(check_complete_case(leaf_count, rng_seed))
}

async fn check_complete_case(leaf_count: usize, rng_seed: u64) {
    let mut rng = StdRng::seed_from_u64(rng_seed);
    let pool = setup().await;

    let replica_id = rng.gen();
    let snapshot = Snapshot::generate(&mut rng, leaf_count);

    let mut root_node = RootNode::create(
        &pool,
        &replica_id,
        VersionVector::new(),
        *snapshot.root_hash(),
        MissingBlocksSummary::default(),
    )
    .await
    .unwrap();

    if leaf_count > 0 {
        super::detect_complete_snapshots(&pool, root_node.hash, 0)
            .await
            .unwrap();
        root_node.reload(&pool).await.unwrap();
        assert!(!root_node.is_complete);
    }

    // TODO: consider randomizing the order the nodes are saved so it's not always
    // breadth-first.

    for layer in snapshot.inner_layers() {
        for (parent_hash, nodes) in layer.inner_maps() {
            nodes.save(&pool, &parent_hash).await.unwrap();
            super::detect_complete_snapshots(&pool, *parent_hash, layer.number())
                .await
                .unwrap();
            root_node.reload(&pool).await.unwrap();
            assert!(!root_node.is_complete);
        }
    }

    let mut unsaved_leaves = snapshot.leaf_count();

    for (parent_hash, nodes) in snapshot.leaf_sets() {
        nodes.save(&pool, &parent_hash).await.unwrap();
        unsaved_leaves -= nodes.len();

        super::detect_complete_snapshots(&pool, *parent_hash, INNER_LAYER_COUNT)
            .await
            .unwrap();
        root_node.reload(&pool).await.unwrap();

        if unsaved_leaves > 0 {
            assert!(!root_node.is_complete);
        }
    }

    assert!(root_node.is_complete);
}

#[ignore]
#[proptest]
fn missing_blocks(
    #[strategy(0usize..=32)] leaf_count: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(missing_blocks_case(leaf_count, rng_seed))
}

async fn missing_blocks_case(leaf_count: usize, rng_seed: u64) {
    let mut rng = StdRng::seed_from_u64(rng_seed);
    let pool = setup().await;

    let replica_id = rng.gen();
    let block_ids: Vec<_> = (&mut rng).sample_iter(Standard).take(leaf_count).collect();
    let leaves = block_ids
        .iter()
        .map(|block_id| {
            let locator = rng.gen::<u64>().hash();
            LeafNode::missing(locator, *block_id)
        })
        .collect();
    let snapshot = Snapshot::from_leaves(leaves);

    // Save the snapshot initially with all nodes missing.
    let mut root_node = RootNode::create(
        &pool,
        &replica_id,
        VersionVector::new(),
        *snapshot.root_hash(),
        MissingBlocksSummary::ALL,
    )
    .await
    .unwrap();

    for layer in snapshot.inner_layers() {
        for (parent_hash, nodes) in layer.inner_maps() {
            nodes
                .clone()
                .into_missing()
                .save(&pool, &parent_hash)
                .await
                .unwrap();
        }
    }

    for (parent_hash, nodes) in snapshot.leaf_sets() {
        nodes
            .clone()
            .into_missing()
            .save(&pool, &parent_hash)
            .await
            .unwrap();

        // This also updates missing blocks
        super::detect_complete_snapshots(&pool, *parent_hash, INNER_LAYER_COUNT)
            .await
            .unwrap();
    }

    let mut expected_missing_blocks_count = block_ids.len() as u64;

    // Check that initially all blocks are missing
    root_node.reload(&pool).await.unwrap();
    assert_eq!(
        root_node.missing_blocks.count,
        expected_missing_blocks_count
    );

    for block_id in block_ids {
        let mut tx = pool.begin().await.unwrap();
        super::receive_block(&mut tx, &block_id).await.unwrap();
        tx.commit().await.unwrap();

        expected_missing_blocks_count -= 1;

        root_node.reload(&pool).await.unwrap();
        assert_eq!(
            root_node.missing_blocks.count,
            expected_missing_blocks_count
        );

        // TODO: check also inner and leaf nodes
    }
}

async fn setup() -> db::Pool {
    let pool = db::Pool::connect(":memory:").await.unwrap();
    super::super::init(&pool).await.unwrap();
    pool
}
