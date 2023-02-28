use super::{
    client::Client,
    message::{Content, Request, Response},
    repository_stats::RepositoryStats,
    request::MAX_PENDING_REQUESTS,
    server::Server,
};
use crate::{
    block::{self, BlockId, BlockTracker, BLOCK_SIZE},
    crypto::sign::{Keypair, PublicKey},
    db,
    event::Event,
    index::{
        node_test_utils::{receive_blocks, receive_nodes, Snapshot},
        BranchData, Index, RootNode, SingleBlockPresence, VersionVectorOp,
    },
    repository::{LocalId, RepositoryId},
    store::{BlockRequestMode, Store},
    test_utils,
    version_vector::VersionVector,
};
use futures_util::future;
use rand::prelude::*;
use std::{fmt, future::Future, sync::Arc};
use tempfile::TempDir;
use test_strategy::proptest;
use tokio::{
    pin, select,
    sync::{
        broadcast::{self, error::RecvError},
        mpsc, Semaphore,
    },
    time::{self, Duration},
};
use tracing::{Instrument, Span};

const TIMEOUT: Duration = Duration::from_secs(50);

// Test complete transfer of one snapshot from one replica to another
// Also test a new snapshot transfer is performed after every local branch
// change.
//
// NOTE: Reducing the number of cases otherwise this test is too slow.
// TODO: Make it faster and increase the cases.
#[proptest(cases = 8)]
fn transfer_snapshot_between_two_replicas(
    #[strategy(0usize..32)] leaf_count: usize,
    #[strategy(0usize..2)] changeset_count: usize,
    #[strategy(1usize..4)] changeset_size: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(transfer_snapshot_between_two_replicas_case(
        leaf_count,
        changeset_count,
        changeset_size,
        rng_seed,
    ))
}

async fn transfer_snapshot_between_two_replicas_case(
    leaf_count: usize,
    changeset_count: usize,
    changeset_size: usize,
    rng_seed: u64,
) {
    assert!(changeset_size > 0);

    let mut rng = StdRng::seed_from_u64(rng_seed);

    let write_keys = Keypair::generate(&mut rng);
    let (_a_base_dir, a_store, a_id) = create_store(&mut rng, &write_keys).await;
    let (_b_base_dir, b_store, _) = create_store(&mut rng, &write_keys).await;

    let snapshot = Snapshot::generate(&mut rng, leaf_count);
    save_snapshot(&a_store.index, a_id, &write_keys, &snapshot).await;
    receive_blocks(&a_store, &snapshot).await;

    assert!(load_latest_root_node(&b_store.index, a_id).await.is_none());

    let mut server = create_server(a_store.index.clone());
    let mut client = create_client(b_store.clone());

    // Wait until replica B catches up to replica A, then have replica A perform a local change
    // and repeat.
    let drive = async {
        let mut remaining_changesets = changeset_count;

        loop {
            wait_until_snapshots_in_sync(&a_store.index, a_id, &b_store.index).await;

            if remaining_changesets > 0 {
                create_changeset(&mut rng, &a_store.index, &a_id, &write_keys, changeset_size)
                    .await;
                remaining_changesets -= 1;
            } else {
                break;
            }
        }
    };

    simulate_connection_until(&mut server, &mut client, drive).await;

    // HACK: prevent "too many open files" error.
    a_store.db().close().await.unwrap();
    b_store.db().close().await.unwrap();
}

// NOTE: Reducing the number of cases otherwise this test is too slow.
// TODO: Make it faster and increase the cases.
#[proptest(cases = 8)]
fn transfer_blocks_between_two_replicas(
    #[strategy(1usize..32)] block_count: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(transfer_blocks_between_two_replicas_case(
        block_count,
        rng_seed,
    ))
}

async fn transfer_blocks_between_two_replicas_case(block_count: usize, rng_seed: u64) {
    let mut rng = StdRng::seed_from_u64(rng_seed);

    let write_keys = Keypair::generate(&mut rng);
    let (_a_base_dir, a_store, a_id) = create_store(&mut rng, &write_keys).await;
    let (_b_base_dir, b_store, b_id) = create_store(&mut rng, &write_keys).await;

    // Initially both replicas have the whole snapshot but no blocks.
    let snapshot = Snapshot::generate(&mut rng, block_count);
    save_snapshot(&a_store.index, a_id, &write_keys, &snapshot).await;
    save_snapshot(&b_store.index, b_id, &write_keys, &snapshot).await;

    let mut server = create_server(a_store.index.clone());
    let mut client = create_client(b_store.clone());

    // Keep adding the blocks to replica A and verify they get received by replica B as well.
    let drive = async {
        for (id, block) in snapshot.blocks() {
            // Write the block by replica A.
            a_store
                .write_received_block(&block.data, &block.nonce)
                .await
                .unwrap();

            // Then wait until replica B receives and writes it too.
            wait_until_block_exists_(&b_store.index, &a_store.index, id).await;
        }
    };

    simulate_connection_until(&mut server, &mut client, drive).await;

    drop(client);

    // HACK: prevent "too many open files" error.
    a_store.db().close().await.unwrap();
    b_store.db().close().await.unwrap();
}

// Receive a `LeafNode` with non-missing block, then drop the connection before the block itself is
// received, then re-establish the connection and make sure the block gets received then.
#[tokio::test]
async fn failed_block_only_peer() {
    let mut rng = StdRng::seed_from_u64(0);

    let write_keys = Keypair::generate(&mut rng);
    let (_a_base_dir, a_store, a_id) = create_store(&mut rng, &write_keys).await;
    let (_a_base_dir, b_store, _) = create_store(&mut rng, &write_keys).await;

    let snapshot = Snapshot::generate(&mut rng, 1);
    save_snapshot(&a_store.index, a_id, &write_keys, &snapshot).await;
    receive_blocks(&a_store, &snapshot).await;

    let mut server = create_server(a_store.index.clone());
    let mut client = create_client(b_store.clone());

    simulate_connection_until(
        &mut server,
        &mut client,
        wait_until_snapshots_in_sync(&a_store.index, a_id, &b_store.index),
    )
    .await;

    // Simulate peer disconnecting and reconnecting.
    drop(server);
    drop(client);

    let mut server = create_server(a_store.index.clone());
    let mut client = create_client(b_store.clone());

    simulate_connection_until(&mut server, &mut client, async {
        for id in snapshot.blocks().keys() {
            wait_until_block_exists(&b_store.index, id).await
        }
    })
    .await;
}

// Same as `failed_block_only_peer` test but this time there is a second peer who remains connected
// for the whole duration of the test. This is to uncover any potential request caching issues.
#[tokio::test]
async fn failed_block_same_peer() {
    let mut rng = StdRng::seed_from_u64(0);

    let write_keys = Keypair::generate(&mut rng);
    let (_a_base_dir, a_store, a_id) = create_store(&mut rng, &write_keys).await;
    let (_b_base_dir, b_store, _) = create_store(&mut rng, &write_keys).await;
    let (_c_base_dir, c_store, _) = create_store(&mut rng, &write_keys).await;

    let snapshot = Snapshot::generate(&mut rng, 1);
    save_snapshot(&a_store.index, a_id, &write_keys, &snapshot).await;
    receive_blocks(&a_store, &snapshot).await;

    // [A]-(server_ac)---+
    //                   |
    //               (client_ca)
    //                   |
    //                  [C]
    //                   |
    //               (client_cb)
    //                   |
    // [B]-(server_bc)---+

    let mut server_ac = create_server(a_store.index.clone());
    let mut client_ca = create_client(c_store.clone());

    let mut server_bc = create_server(b_store.index.clone());
    let mut client_cb = create_client(c_store.clone());

    // Run both connections in parallel until C syncs its index (but not blocks) with A
    let conn_ac = simulate_connection(&mut server_ac, &mut client_ca);
    let conn_ac = conn_ac.instrument(tracing::info_span!("AC1"));

    let conn_bc = simulate_connection(&mut server_bc, &mut client_cb);
    let conn_bc = conn_bc.instrument(tracing::info_span!("BC"));
    pin!(conn_bc);

    run_until(
        future::join(conn_ac, &mut conn_bc),
        wait_until_snapshots_in_sync(&a_store.index, a_id, &c_store.index),
    )
    .await;

    // Drop and recreate the A-C connection but keep the B-C connection up.
    drop(server_ac);
    drop(client_ca);

    let mut server_ac = create_server(a_store.index.clone());
    let mut client_ca = create_client(c_store.clone());

    // Run the new A-C connection in parallel with the existing B-C connection until all blocks are
    // received.
    let conn_ac = simulate_connection(&mut server_ac, &mut client_ca);
    let conn_ac = conn_ac.instrument(tracing::info_span!("AC2"));

    run_until(future::join(conn_ac, conn_bc), async {
        for id in snapshot.blocks().keys() {
            wait_until_block_exists(&c_store.index, id).await
        }
    })
    .await;
}

#[tokio::test]
async fn failed_block_other_peer() {
    for _ in 0..1000 {
        failed_block_other_peer_().await;
    }
}
// This test verifies that when there are two peers that have a particular block, even when one of
// them drops, we can still succeed in retrieving the block from the remaining peer.
async fn failed_block_other_peer_() {
    let mut i: u32 = 0;
    // This test has a delicate setup phase which might not always succeed (it's not
    // deterministic) so the setup might need to be repeated multiple times.
    'main: loop {
        i += 1;
        println!(
            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> START {} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
            i
        );
        let mut rng = StdRng::seed_from_u64(0);

        let write_keys = Keypair::generate(&mut rng);
        let (_a_base_dir, a_store, a_id) = create_store(&mut rng, &write_keys).await;
        let (_b_base_dir, b_store, b_id) = create_store(&mut rng, &write_keys).await;
        let (_c_base_dir, c_store, _) = create_store(&mut rng, &write_keys).await;

        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        // Create the snapshot by A
        let snapshot = Snapshot::generate(&mut rng, 1);
        save_snapshot(&a_store.index, a_id, &write_keys, &snapshot).await;
        receive_blocks(&a_store, &snapshot).await;

        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        //let handle = tokio::task::spawn(async move {
        //    tokio::task::yield_now().await;

        //    use futures_util::TryStreamExt;
        //    let mut conn = a_store.index.pool.acquire().await.unwrap();
        //    let vec: Vec<RootNode> = RootNode::load_all_latest_complete(&mut conn)
        //        .try_collect()
        //        .await
        //        .unwrap();

        //    assert!(!vec.is_empty());
        //});
        //handle.await.unwrap();
        //return;
        // Sync B with A
        let mut server_ab = create_server(a_store.index.clone());
        let mut client_ba = create_client_(b_store.clone(), format!("ba:{}", i));
        simulate_connection_until_(
            &mut server_ab,
            &mut client_ba,
            async {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                //for id in snapshot.blocks().keys() {
                //    wait_until_block_exists_(&b_store.index, &a_store.index, id).await;
                //}
            },
            format!("ab:{}", i),
        )
        .await;
        //select! {
        //    result = server_ab.0.run() => result.unwrap(),
        //    _ = tokio::time::sleep(std::time::Duration::from_millis(1000)) => {}
        //}
        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        drop(server_ab);
        drop(client_ba);
        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        return;

        // [A]-(server_ac)---+
        //                   |
        //               (client_ca)
        //                   |
        //                  [C]
        //                   |
        //               (client_cb)
        //                   |
        // [B]-(server_bc)---+

        let mut server_ac = create_server(a_store.index.clone());
        let mut client_ca = create_client_(c_store.clone(), format!("ca:{}", i));

        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        let mut server_bc = create_server(b_store.index.clone());
        let mut client_cb = create_client_(c_store.clone(), format!("cb:{}", i));

        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        // Run the two connections in parallel until C syncs its index with both A and B.
        let conn_bc = simulate_connection(&mut server_bc, &mut client_cb);
        let conn_bc = conn_bc.instrument(tracing::info_span!("BC"));
        let mut conn_bc = Box::pin(conn_bc);

        let conn_ac = simulate_connection(&mut server_ac, &mut client_ca);
        let conn_ac = conn_ac.instrument(tracing::info_span!("AC"));

        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        run_until(future::join(conn_ac, &mut conn_bc), async {
            wait_until_snapshots_in_sync(&a_store.index, a_id, &c_store.index).await;
            wait_until_snapshots_in_sync(&b_store.index, b_id, &c_store.index).await;
        })
        .await;

        println!(">>>>>>>>>>>>>>>>>>>>>> L{}", line!());
        // Drop the A-C connection so C can't receive any blocks from A anymore.
        drop(server_ac);
        drop(client_ca);

        println!(">>>>>>>>>>>>>>>>>>>>>> Checking precondition");
        // It might sometimes happen that the block were already received in the previous step
        // In that case the situation this test is trying to exercise does not occur and we need
        // to try again.
        let mut conn = c_store.db().acquire().await.unwrap();
        for id in snapshot.blocks().keys() {
            if block::exists(&mut conn, id).await.unwrap() {
                println!(">>>>>>>>>>>>>>>>>>>>>> Checking failed");
                tracing::warn!("test preconditions not met, trying again");

                drop(conn);
                drop(conn_bc);

                a_store.db().close().await.unwrap();
                b_store.db().close().await.unwrap();
                c_store.db().close().await.unwrap();

                continue 'main;
            }
        }
        drop(conn);

        println!(">>>>>>>>>>>>>>>>>>>>>> Checking passed");
        // Continue running the B-C connection and verify C receives the missing blocks from B who is
        // the only remaining peer at this point.
        run_until(conn_bc, async {
            for id in snapshot.blocks().keys() {
                wait_until_block_exists(&c_store.index, id).await;
            }
        })
        .await;

        break;
    }
}

async fn create_store<R: Rng + CryptoRng>(
    rng: &mut R,
    write_keys: &Keypair,
) -> (TempDir, Store, PublicKey) {
    let (base_dir, db) = db::create_temp().await.unwrap();
    let writer_id = PublicKey::generate(rng);
    let repository_id = RepositoryId::from(write_keys.public);
    let (event_tx, _) = broadcast::channel(1);

    let index = Index::new(db, repository_id, event_tx);
    // index.create_branch(writer_id, write_keys).await.unwrap();

    let store = Store {
        index,
        block_tracker: BlockTracker::new(),
        block_request_mode: BlockRequestMode::Greedy,
        local_id: LocalId::new(),
    };

    (base_dir, store, writer_id)
}

// Enough capacity to prevent deadlocks.
// TODO: find the actual minimum necessary capacity.
const CAPACITY: usize = 256;

async fn save_snapshot(
    index: &Index,
    writer_id: PublicKey,
    write_keys: &Keypair,
    snapshot: &Snapshot,
) {
    // If the snapshot is empty then there is nothing else to save in addition to the initial root
    // node the index already has.
    if snapshot.leaf_count() == 0 {
        return;
    }

    let mut version_vector = VersionVector::new();
    version_vector.insert(writer_id, 2); // to force overwrite the initial root node

    receive_nodes(index, write_keys, writer_id, version_vector, snapshot).await;
}

async fn wait_until_snapshots_in_sync(
    server_index: &Index,
    server_id: PublicKey,
    client_index: &Index,
) {
    let mut rx = client_index.subscribe();

    let server_root = load_latest_root_node(server_index, server_id).await;
    let server_root = if let Some(server_root) = server_root {
        server_root
    } else {
        return;
    };

    if server_root.proof.version_vector.is_empty() {
        return;
    }

    loop {
        if let Some(client_root) = load_latest_root_node(client_index, server_id).await {
            if client_root.summary.is_complete() && client_root.proof.hash == server_root.proof.hash
            {
                // client has now fully downloaded server's latest snapshot.
                assert_eq!(
                    client_root.proof.version_vector,
                    server_root.proof.version_vector
                );
                break;
            }
        }

        recv_any(&mut rx).await
    }
}

async fn wait_until_block_exists(index: &Index, block_id: &BlockId) {
    println!("wait_until_block_exists start and checking {}", line!());

    let mut rx = index.subscribe();

    if block::exists(&mut index.pool.acquire().await.unwrap(), block_id)
        .await
        .unwrap()
    {
        println!("wait_until_block_exists end - block exists {}", line!());
        return;
    }
    println!("wait_until_block_exists it doesn't yet {}", line!());

    //while !block::exists(&mut index.pool.acquire().await.unwrap(), block_id)
    //    .await
    //    .unwrap()
    //{
    //    println!("wait_until_block_exists waiting {}", line!());
    //    recv_any(&mut rx).await {
    //    println!("wait_until_block_exists checking {}", line!());
    //}
    let mut i = 0;
    loop {
        select! {
            _ = recv_any(&mut rx) => {
                let mut conn = index.pool.acquire().await.unwrap();
                let exists = block::exists(&mut conn, block_id).await.unwrap();

                if exists {
                    println!("wait_until_block_exists end {} exists", line!());
                    break;
                } else {
                    println!("wait_until_block_exists got notification {} but block doesn't exist yet", line!());
                    index.dump_with(&mut conn).await;
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                let exists = block::exists(&mut index.pool.acquire().await.unwrap(), block_id).await.unwrap();
                println!("wait_until_block_exists timeout {} exists:{:?}", line!(), exists);
                i += 1;
                index.dump().await;
                if i > 30 {
                    panic!();
                }
            }
        }
    }
}

async fn wait_until_block_exists_(index_b: &Index, index_a: &Index, block_id: &BlockId) {
    println!("wait_until_block_exists start and checking {}", line!());

    let mut rx = index_b.subscribe();

    if block::exists(&mut index_b.pool.acquire().await.unwrap(), block_id)
        .await
        .unwrap()
    {
        println!("wait_until_block_exists end - block exists {}", line!());
        return;
    }
    println!("wait_until_block_exists it doesn't yet {}", line!());

    //while !block::exists(&mut index.pool.acquire().await.unwrap(), block_id)
    //    .await
    //    .unwrap()
    //{
    //    println!("wait_until_block_exists waiting {}", line!());
    //    recv_any(&mut rx).await {
    //    println!("wait_until_block_exists checking {}", line!());
    //}
    let mut i = 0;
    loop {
        select! {
            _ = recv_any(&mut rx) => {
                let exists = block::exists(&mut index_b.pool.acquire().await.unwrap(), block_id).await.unwrap();

                if exists {
                    println!("wait_until_block_exists end {} exists", line!());
                    break;
                } else {
                    println!("wait_until_block_exists got notification {} but block doesn't exist yet", line!());
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                let exists = block::exists(&mut index_b.pool.acquire().await.unwrap(), block_id).await.unwrap();
                println!("wait_until_block_exists timeout {} exists:{:?}", line!(), exists);
                i += 1;
                println!(">>>>>>>>>>>>>>>>>>>>>> INDEX A <<<<<<<<<<<<<<<<<");
                index_a.dump().await;
                println!(">>>>>>>>>>>>>>>>>>>>>> INDEX B <<<<<<<<<<<<<<<<<");
                index_b.dump().await;
                if i > 30 {
                    panic!();
                }
            }
        }
    }
}

async fn recv_any(rx: &mut broadcast::Receiver<Event>) {
    match rx.recv().await {
        Ok(_) | Err(RecvError::Lagged(_)) => (),
        Err(RecvError::Closed) => panic!("event channel unexpectedly closed"),
    }
}

// Simulate a changeset, e.g. create a file, write to it and flush it.
async fn create_changeset(
    rng: &mut StdRng,
    index: &Index,
    writer_id: &PublicKey,
    write_keys: &Keypair,
    size: usize,
) {
    let branch = index.get_branch(*writer_id);

    for _ in 0..size {
        create_block(rng, index, &branch, write_keys).await;
    }

    let mut tx = index.pool.begin_write().await.unwrap();
    branch
        .bump(&mut tx, &VersionVectorOp::IncrementLocal, write_keys)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    branch.notify();
}

async fn create_block(rng: &mut StdRng, index: &Index, branch: &BranchData, write_keys: &Keypair) {
    let encoded_locator = rng.gen();

    let mut content = vec![0; BLOCK_SIZE];
    rng.fill(&mut content[..]);

    let block_id = BlockId::from_content(&content);
    let nonce = rng.gen();

    let mut tx = index.pool.begin_write().await.unwrap();
    branch
        .insert(
            &mut tx,
            &encoded_locator,
            &block_id,
            SingleBlockPresence::Present,
            write_keys,
        )
        .await
        .unwrap();
    block::write(&mut tx, &block_id, &content, &nonce)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn load_latest_root_node(index: &Index, writer_id: PublicKey) -> Option<RootNode> {
    RootNode::load_latest_by_writer(&mut index.pool.acquire().await.unwrap(), writer_id)
        .await
        .unwrap()
}

// Simulate connection between two replicas until the given future completes.
async fn simulate_connection_until<F>(server: &mut ServerData, client: &mut ClientData, until: F)
where
    F: Future,
{
    run_until(simulate_connection(server, client), until).await
}

async fn simulate_connection_until_<F>(
    server: &mut ServerData,
    client: &mut ClientData,
    until: F,
    conn_name: String,
) where
    F: Future,
{
    run_until(simulate_connection_(server, client, conn_name), until).await
}

// Simulate connection forever.
async fn simulate_connection(server: &mut ServerData, client: &mut ClientData) {
    let (server, server_send_rx, server_recv_tx) = server;
    let (client, client_send_rx, client_recv_tx) = client;

    let mut server_conn = Connection {
        send_rx: server_send_rx,
        recv_tx: client_recv_tx,
    };

    let mut client_conn = Connection {
        send_rx: client_send_rx,
        recv_tx: server_recv_tx,
    };

    select! {
        biased; // deterministic poll order for repeatable tests

        result = server.run() => result.unwrap(),
        result = client.run() => result.unwrap(),
        _ = server_conn.run() => panic!("connection closed prematurely"),
        _ = client_conn.run() => panic!("connection closed prematurely"),
    }
}

// Simulate connection forever.
async fn simulate_connection_(server: &mut ServerData, client: &mut ClientData, conn_name: String) {
    let (server, server_send_rx, server_recv_tx) = server;
    let (client, client_send_rx, client_recv_tx) = client;

    let mut server_conn = Connection_ {
        send_rx: server_send_rx,
        recv_tx: client_recv_tx,
        conn_name: format!("{}:server->client", conn_name),
    };

    let mut client_conn = Connection_ {
        send_rx: client_send_rx,
        recv_tx: server_recv_tx,
        conn_name: format!("{}:client->server", conn_name),
    };

    select! {
        biased; // deterministic poll order for repeatable tests

        result = server.run() => result.unwrap(),
        result = client.run() => result.unwrap(),
        _ = server_conn.run() => panic!("connection closed prematurely"),
        //_ = client_conn.run() => panic!("connection closed prematurely"),
    }
}

// Runs `task` until `until` completes. Panics if `until` doesn't complete before `TIMEOUT` or if
// `task` completes before `until`.
async fn run_until<F, U>(task: F, until: U)
where
    F: Future,
    U: Future,
{
    select! {
        biased; // deterministic poll order for repeatable tests
        _ = task => panic!("task completed prematurely"),
        _ = until => (),
        _ = time::sleep(TIMEOUT) => panic!("test timed out"),
    }
}

type ServerData = (Server, mpsc::Receiver<Content>, mpsc::Sender<Request>);
type ClientData = (Client, mpsc::Receiver<Content>, mpsc::Sender<Response>);

fn create_server(index: Index) -> ServerData {
    let (send_tx, send_rx) = mpsc::channel(1);
    let (recv_tx, recv_rx) = mpsc::channel(CAPACITY);
    let server = Server::new(index, send_tx, recv_rx);

    (server, send_rx, recv_tx)
}

fn create_client_(store: Store, client_name: String) -> ClientData {
    let (send_tx, send_rx) = mpsc::channel(1);
    let (recv_tx, recv_rx) = mpsc::channel(CAPACITY);
    let client = Client::new_(
        store,
        send_tx,
        recv_rx,
        Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
        Arc::new(RepositoryStats::new(Span::none())),
        client_name.clone(),
    );

    (client, send_rx, recv_tx)
}

fn create_client(store: Store) -> ClientData {
    let (send_tx, send_rx) = mpsc::channel(1);
    let (recv_tx, recv_rx) = mpsc::channel(CAPACITY);
    let client = Client::new(
        store,
        send_tx,
        recv_rx,
        Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
        Arc::new(RepositoryStats::new(Span::none())),
    );

    (client, send_rx, recv_tx)
}

// Simulated connection between a server and a client.
struct Connection<'a, T> {
    send_rx: &'a mut mpsc::Receiver<Content>,
    recv_tx: &'a mut mpsc::Sender<T>,
}

impl<T> Connection<'_, T>
where
    T: From<Content> + fmt::Debug,
{
    async fn run(&mut self) {
        while let Some(content) = self.send_rx.recv().await {
            self.recv_tx.send(content.into()).await.unwrap();
        }
    }
}

struct Connection_<'a, T> {
    send_rx: &'a mut mpsc::Receiver<Content>,
    recv_tx: &'a mut mpsc::Sender<T>,
    conn_name: String,
}

impl<T> Connection_<'_, T>
where
    T: From<Content> + fmt::Debug,
{
    async fn run(&mut self) {
        while let Some(content) = self.send_rx.recv().await {
            println!("{} : {:?}", self.conn_name, content);
            self.recv_tx.send(content.into()).await.unwrap();
        }
    }
}
