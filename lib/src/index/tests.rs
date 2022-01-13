use super::{node::RootNode, node_test_utils::Snapshot, *};
use crate::crypto::sign::{Keypair, PublicKey};
use assert_matches::assert_matches;

#[tokio::test(flavor = "multi_thread")]
async fn receive_valid_root_node() {
    let (index, write_keys) = setup().await;

    let local_id = PublicKey::random();
    let remote_id = PublicKey::random();

    index
        .create_branch(Proof::first(local_id, &write_keys))
        .await
        .unwrap();

    // Initially only the local branch exists
    let mut conn = index.pool.acquire().await.unwrap();
    assert!(RootNode::load_latest(&mut conn, local_id)
        .await
        .unwrap()
        .is_some());
    assert!(RootNode::load_latest(&mut conn, remote_id)
        .await
        .unwrap()
        .is_none());
    drop(conn);

    // Receive root node from the remote replica.
    index
        .receive_root_node(
            Proof::first(remote_id, &write_keys).into(),
            VersionVector::first(remote_id),
            Summary::INCOMPLETE,
        )
        .await
        .unwrap();

    // Both the local and the remote branch now exist.
    let mut conn = index.pool.acquire().await.unwrap();
    assert!(RootNode::load_latest(&mut conn, local_id)
        .await
        .unwrap()
        .is_some());
    assert!(RootNode::load_latest(&mut conn, remote_id)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_root_node_with_invalid_proof() {
    let (index, write_keys) = setup().await;

    let local_id = PublicKey::random();
    let remote_id = PublicKey::random();

    index
        .create_branch(Proof::first(local_id, &write_keys))
        .await
        .unwrap();

    // Receive invalid root node from the remote replica.
    let invalid_write_keys = Keypair::random();
    let result = index
        .receive_root_node(
            Proof::first(remote_id, &invalid_write_keys).into(),
            VersionVector::first(remote_id),
            Summary::INCOMPLETE,
        )
        .await;
    assert_matches!(result, Err(ReceiveError::InvalidProof));

    // The invalid root was not written to the db.
    let mut conn = index.pool.acquire().await.unwrap();
    assert!(RootNode::load_latest(&mut conn, remote_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_valid_inner_nodes() {
    let (index, write_keys) = setup().await;

    let local_id = PublicKey::random();
    let remote_id = PublicKey::random();

    index
        .create_branch(Proof::first(local_id, &write_keys))
        .await
        .unwrap();

    let snapshot = Snapshot::generate(&mut rand::thread_rng(), 1);

    index
        .receive_root_node(
            Proof::new(remote_id, *snapshot.root_hash(), &write_keys).into(),
            VersionVector::first(remote_id),
            Summary::INCOMPLETE,
        )
        .await
        .unwrap();
    let inner_nodes = snapshot
        .inner_layers()
        .next()
        .unwrap()
        .inner_maps()
        .next()
        .unwrap()
        .1
        .clone();
    index.receive_inner_nodes(inner_nodes).await.unwrap();

    let inner_nodes = InnerNode::load_children(
        &mut index.pool.acquire().await.unwrap(),
        snapshot.root_hash(),
    )
    .await
    .unwrap();
    assert!(!inner_nodes.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_inner_nodes_with_missing_root_parent() {
    let (index, write_keys) = setup().await;

    let local_id = PublicKey::random();

    index
        .create_branch(Proof::first(local_id, &write_keys))
        .await
        .unwrap();

    let snapshot = Snapshot::generate(&mut rand::thread_rng(), 1);

    for layer in snapshot.inner_layers() {
        let inner_nodes = layer.inner_maps().next().unwrap().1.clone();
        let result = index.receive_inner_nodes(inner_nodes).await;
        assert_matches!(result, Err(ReceiveError::ParentNodeNotFound));

        // The orphaned inner nodes were not written to the db.
        let inner_nodes = InnerNode::load_children(
            &mut index.pool.acquire().await.unwrap(),
            snapshot.root_hash(),
        )
        .await
        .unwrap();
        assert!(inner_nodes.is_empty());
    }
}

async fn setup() -> (Index, Keypair) {
    let pool = db::open_or_create(&db::Store::Memory).await.unwrap();
    init(&mut pool.acquire().await.unwrap()).await.unwrap();

    let write_keys = Keypair::random();
    let repository_id = RepositoryId::from(write_keys.public);
    let index = Index::load(pool, repository_id).await.unwrap();

    (index, write_keys)
}
