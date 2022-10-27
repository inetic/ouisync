use super::*;
use crate::{blob, block::BLOCK_SIZE, db, scoped_task};
use assert_matches::assert_matches;
use rand::Rng;
use std::io::SeekFrom;
use tempfile::TempDir;
use tokio::time::{sleep, timeout, Duration};

#[tokio::test(flavor = "multi_thread")]
async fn root_directory_always_exists() {
    let base_dir = TempDir::new().unwrap();
    let writer_id = rand::random();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        writer_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();
    let _ = repo.open_directory("/").await.unwrap();
}

// Count leaf nodes in the index of the local branch.
async fn count_local_index_leaf_nodes(repo: &Repository) -> usize {
    let store = repo.store();
    let branch = repo.local_branch().unwrap();
    let mut conn = store.db().acquire().await.unwrap();
    branch.data().count_leaf_nodes(&mut conn).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn count_leaf_nodes_sanity_checks() {
    let base_dir = TempDir::new().unwrap();
    let device_id = rand::random();

    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        device_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();

    let file_name = "test.txt";

    //------------------------------------------------------------------------
    // Create a small file in the root.

    let mut file = repo.create_file(file_name).await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, &random_bytes(BLOCK_SIZE - blob::HEADER_SIZE))
        .await
        .unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    // 2 = one for the root + one for the file.
    assert_eq!(count_local_index_leaf_nodes(&repo).await, 2);

    //------------------------------------------------------------------------
    // Make the file bigger to expand to two blocks

    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, &random_bytes(1)).await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    // 3 = one for the root + two for the file.
    assert_eq!(count_local_index_leaf_nodes(&repo).await, 3);

    //------------------------------------------------------------------------
    // Remove the file, we should end up with just one block for the root.

    drop(file);
    repo.remove_entry(file_name).await.unwrap();

    // 1 = one for the root with a tombstone entry
    assert_eq!(count_local_index_leaf_nodes(&repo).await, 1);

    //------------------------------------------------------------------------
}

#[tokio::test(flavor = "multi_thread")]
async fn merge() {
    let base_dir = TempDir::new().unwrap();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        rand::random(),
        MasterSecret::random(),
        AccessSecrets::random_write(),
        true,
    )
    .await
    .unwrap();

    // Create remote branch and create a file in it.
    let remote_id = PublicKey::random();
    create_remote_file(&repo, remote_id, "test.txt", b"hello").await;

    // Open the local root.
    let local_branch = repo.local_branch().unwrap();
    let mut local_root = {
        let mut conn = repo.db().acquire().await.unwrap();
        local_branch.open_or_create_root(&mut conn).await.unwrap()
    };

    repo.force_merge().await.unwrap();

    let mut conn = repo.db().acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();
    let content = local_root
        .lookup("test.txt")
        .unwrap()
        .file()
        .unwrap()
        .open(&mut conn)
        .await
        .unwrap()
        .read_to_end(&mut conn)
        .await
        .unwrap();
    assert_eq!(content, b"hello");
}

#[tokio::test(flavor = "multi_thread")]
async fn recreate_previously_deleted_file() {
    let base_dir = TempDir::new().unwrap();
    let local_id = rand::random();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        local_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();

    // Create file
    let mut file = repo.create_file("test.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, b"foo").await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    drop(file);

    // Read it back and check the content
    let content = read_file(&repo, "test.txt").await;
    assert_eq!(content, b"foo");

    // Delete it and assert it's gone
    repo.remove_entry("test.txt").await.unwrap();
    assert_matches!(repo.open_file("test.txt").await, Err(Error::EntryNotFound));

    // Create a file with the same name but different content
    let mut file = repo.create_file("test.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, b"bar").await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    drop(file);

    // Read it back and check the content
    let content = read_file(&repo, "test.txt").await;
    assert_eq!(content, b"bar");
}

#[tokio::test(flavor = "multi_thread")]
async fn recreate_previously_deleted_directory() {
    let base_dir = TempDir::new().unwrap();
    let local_id = rand::random();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        local_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();

    // Create dir
    repo.create_directory("test").await.unwrap();

    // Check it exists
    assert_matches!(repo.open_directory("test").await, Ok(_));

    // Delete it and assert it's gone
    repo.remove_entry("test").await.unwrap();
    assert_matches!(repo.open_directory("test").await, Err(Error::EntryNotFound));

    // Create another directory with the same name
    repo.create_directory("test").await.unwrap();

    // Check it exists
    assert_matches!(repo.open_directory("test").await, Ok(_));
}

// This one used to deadlock
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_read_and_create_dir() {
    let base_dir = TempDir::new().unwrap();
    let writer_id = rand::random();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        writer_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();

    let path = "/dir";
    let repo = Arc::new(repo);

    // The deadlock here happened because the reader lock when opening the directory is
    // acquired in the opposite order to the writer lock acqurired from flushing. I.e. the
    // reader lock acquires `/` and then `/dir`, but flushing acquires `/dir` first and
    // then `/`.
    let create_dir = scoped_task::spawn({
        let repo = repo.clone();
        async move {
            repo.create_directory(path).await.unwrap();
        }
    });

    let open_dir = scoped_task::spawn({
        let repo = repo.clone();
        let mut rx = repo.subscribe();

        async move {
            if let Ok(dir) = repo.open_directory(path).await {
                dir.entries().count();
                return;
            }

            match rx.recv().await {
                Ok(_) | Err(RecvError::Lagged(_)) => (),
                Err(RecvError::Closed) => panic!("Event channel unexpectedly closed"),
            }
        }
    });

    timeout(Duration::from_secs(5), async {
        create_dir.await.unwrap();
        open_dir.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_write_and_read_file() {
    let base_dir = TempDir::new().unwrap();
    let writer_id = rand::random();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        writer_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();

    let repo = Arc::new(repo);

    let chunk_size = 1024;
    let chunk_count = 100;

    let write_file = scoped_task::spawn({
        let repo = repo.clone();

        async move {
            let mut file = repo.create_file("test.txt").await.unwrap();
            let mut buffer = vec![0; chunk_size];

            for _ in 0..chunk_count {
                rand::thread_rng().fill(&mut buffer[..]);

                let mut tx = repo.db().begin().await.unwrap();
                file.write(&mut tx, &buffer).await.unwrap();
                tx.commit().await.unwrap();
            }

            let mut tx = repo.db().begin().await.unwrap();
            file.flush(&mut tx).await.unwrap();
            tx.commit().await.unwrap();
        }
    });

    let open_dir = scoped_task::spawn({
        let repo = repo.clone();

        async move {
            loop {
                match repo.open_file("test.txt").await {
                    Ok(file) => {
                        let actual_len = file.len().await;
                        let expected_len = (chunk_count * chunk_size) as u64;

                        if actual_len == expected_len {
                            break;
                        }
                    }
                    Err(Error::EntryNotFound) => (),
                    Err(error) => panic!("unexpected error: {:?}", error),
                };

                sleep(Duration::from_millis(10)).await;
            }
        }
    });

    timeout(Duration::from_secs(5), async {
        write_file.await.unwrap();
        open_dir.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn interleaved_flush() {
    let content1 = random_bytes(BLOCK_SIZE - blob::HEADER_SIZE); // 1 block
    let content2 = random_bytes(2 * BLOCK_SIZE - blob::HEADER_SIZE); // 2 blocks

    case(
        "write 2 blocks, write 0 blocks, flush 2nd then 1st",
        &content2,
        &[],
        FlushOrder::SecondThenFirst,
        3,
        &content2,
    )
    .await;

    case(
        "write 1 block, write 2 blocks, flush 1st then 2nd",
        &content1,
        &content2,
        FlushOrder::FirstThenSecond,
        3,
        &concat([nth_block(&content1, 0), nth_block(&content2, 1)]),
    )
    .await;

    case(
        "write 1 block, write 2 blocks, flush 2nd then 1st",
        &content1,
        &content2,
        FlushOrder::SecondThenFirst,
        3,
        &concat([nth_block(&content1, 0), nth_block(&content2, 1)]),
    )
    .await;

    enum FlushOrder {
        FirstThenSecond,
        SecondThenFirst,
    }

    // Open two instances of the same file, write `content0` to the first and `content1` to the
    // second, then flush in the specified order, finally check the total number of blocks in the
    // repository and the content of the file.
    async fn case(
        label: &str,
        content0: &[u8],
        content1: &[u8],
        flush_order: FlushOrder,
        expected_total_block_count: usize,
        expected_content: &[u8],
    ) {
        let base_dir = TempDir::new().unwrap();
        let repo = Repository::create(
            base_dir.path().join("repo.db"),
            rand::random(),
            MasterSecret::random(),
            AccessSecrets::random_write(),
            false,
        )
        .await
        .unwrap();
        let file_name = "test.txt";

        let mut file0 = repo.create_file(file_name).await.unwrap();
        let mut tx = repo.db().begin().await.unwrap();
        file0.flush(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        let mut file1 = repo.open_file(file_name).await.unwrap();
        let mut tx = repo.db().begin().await.unwrap();

        file0.write(&mut tx, content0).await.unwrap();
        file1.write(&mut tx, content1).await.unwrap();

        match flush_order {
            FlushOrder::FirstThenSecond => {
                file0.flush(&mut tx).await.unwrap();
                file1.flush(&mut tx).await.unwrap();
            }
            FlushOrder::SecondThenFirst => {
                file1.flush(&mut tx).await.unwrap();
                file0.flush(&mut tx).await.unwrap();
            }
        }

        tx.commit().await.unwrap();

        assert_eq!(
            count_local_index_leaf_nodes(&repo).await,
            expected_total_block_count,
            "'{}': unexpected total number of blocks in the repository",
            label
        );

        let actual_content = read_file(&repo, file_name).await;
        assert_eq!(
            actual_content.len(),
            expected_content.len(),
            "'{}': unexpected file content length",
            label
        );

        assert!(
            actual_content == expected_content,
            "'{}': unexpected file content",
            label
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn append_to_file() {
    let (_base_dir, repo) = setup().await;

    let mut file = repo.create_file("foo.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, b"foo").await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    let mut file = repo.open_file("foo.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.seek(&mut tx, SeekFrom::End(0)).await.unwrap();
    file.write(&mut tx, b"bar").await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    let mut file = repo.open_file("foo.txt").await.unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    let content = file.read_to_end(&mut conn).await.unwrap();
    assert_eq!(content, b"foobar");
}

#[tokio::test(flavor = "multi_thread")]
async fn blind_access_non_empty_repo() {
    let (_base_dir, pool) = db::create_temp().await.unwrap();
    let device_id = rand::random();

    // Create the repo and put a file in it.
    let repo = Repository::create_in(
        pool.clone(),
        device_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
        Span::none(),
    )
    .await
    .unwrap();

    let mut file = repo.create_file("secret.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, b"redacted").await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    drop(file);
    drop(repo);

    // Reopen the repo first explicitly in blind mode and then using incorrect secret. The two ways
    // should be indistinguishable from each other.
    for (master_secret, access_mode) in [
        (None, AccessMode::Blind),
        (Some(MasterSecret::random()), AccessMode::Write),
    ] {
        // Reopen the repo in blind mode.
        let repo = Repository::open_in(
            pool.clone(),
            device_id,
            master_secret,
            access_mode,
            false,
            Span::none(),
        )
        .await
        .unwrap();

        // Reading files is not allowed.
        assert_matches!(
            repo.open_file("secret.txt").await,
            Err(Error::PermissionDenied)
        );

        // Creating files is not allowed.
        assert_matches!(
            repo.create_file("hack.txt").await,
            Err(Error::PermissionDenied)
        );

        // Removing files is not allowed.
        assert_matches!(
            repo.remove_entry("secret.txt").await,
            Err(Error::PermissionDenied)
        );

        // Reading the root directory is not allowed either.
        assert_matches!(repo.open_directory("/").await, Err(Error::PermissionDenied));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn blind_access_empty_repo() {
    let (_base_dir, pool) = db::create_temp().await.unwrap();
    let device_id = rand::random();

    // Create an empty repo.
    Repository::create_in(
        pool.clone(),
        device_id,
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
        Span::none(),
    )
    .await
    .unwrap();

    // Reopen the repo in blind mode.
    let repo = Repository::open_in(
        pool.clone(),
        device_id,
        Some(MasterSecret::random()),
        AccessMode::Read,
        false,
        Span::none(),
    )
    .await
    .unwrap();

    // Reading the root directory is not allowed.
    assert_matches!(repo.open_directory("/").await, Err(Error::PermissionDenied));
}

#[tokio::test(flavor = "multi_thread")]
async fn read_access_same_replica() {
    let (_base_dir, pool) = db::create_temp().await.unwrap();
    let device_id = rand::random();
    let master_secret = MasterSecret::random();

    let repo = Repository::create_in(
        pool.clone(),
        device_id,
        master_secret.clone(),
        AccessSecrets::random_write(),
        false,
        Span::none(),
    )
    .await
    .unwrap();

    let mut file = repo.create_file("public.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, b"hello world").await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    drop(file);
    drop(repo);

    // Reopen the repo in read-only mode.
    let repo = Repository::open_in(
        pool,
        device_id,
        Some(master_secret),
        AccessMode::Read,
        false,
        Span::none(),
    )
    .await
    .unwrap();

    // Reading files is allowed.
    let mut file = repo.open_file("public.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();

    let content = file.read_to_end(&mut tx).await.unwrap();
    assert_eq!(content, b"hello world");

    // Writing is not allowed.
    file.seek(&mut tx, SeekFrom::Start(0)).await.unwrap();
    // short writes that don't cross block boundaries don't trigger the permission check which is
    // why the following works...
    file.write(&mut tx, b"hello universe").await.unwrap();
    // ...but flushing the file is not allowed.
    assert_matches!(file.flush(&mut tx).await, Err(Error::PermissionDenied));

    tx.commit().await.unwrap();
    drop(file);

    // Creating files is not allowed.
    assert_matches!(
        repo.create_file("hack.txt").await,
        Err(Error::PermissionDenied)
    );

    // Removing files is not allowed.
    assert_matches!(
        repo.remove_entry("public.txt").await,
        Err(Error::PermissionDenied)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_access_different_replica() {
    let (_base_dir, pool) = db::create_temp().await.unwrap();
    let master_secret = MasterSecret::random();

    let device_id_a = rand::random();
    let repo = Repository::create_in(
        pool.clone(),
        device_id_a,
        master_secret.clone(),
        AccessSecrets::random_write(),
        false,
        Span::none(),
    )
    .await
    .unwrap();

    let mut file = repo.create_file("public.txt").await.unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    file.write(&mut conn, b"hello world").await.unwrap();
    file.flush(&mut conn).await.unwrap();

    drop(file);
    drop(repo);

    let device_id_b = rand::random();
    let repo = Repository::open_in(
        pool,
        device_id_b,
        Some(master_secret),
        AccessMode::Read,
        false,
        Span::none(),
    )
    .await
    .unwrap();

    let mut file = repo.open_file("public.txt").await.unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    let content = file.read_to_end(&mut conn).await.unwrap();
    assert_eq!(content, b"hello world");
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_forked_remote_file() {
    let (_base_dir, repo) = setup().await;

    create_remote_file(&repo, PublicKey::random(), "test.txt", b"foo").await;

    let local_branch = repo.local_branch().unwrap();
    let mut file = repo.open_file("test.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.fork(&mut tx, local_branch).await.unwrap();
    file.truncate(&mut tx, 0).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn attempt_to_modify_remote_file() {
    let (_base_dir, repo) = setup().await;

    create_remote_file(&repo, PublicKey::random(), "test.txt", b"foo").await;

    // HACK: during the above `create_remote_file` call the remote branch is opened in write mode
    // and then the remote root dir is cached. If the garbage collector kicks in at that time and
    // opens the remote root dir as well, it retains it in the cache even after
    // `create_remote_file` is finished. Then the following `open_file` might open the root from
    // the cache where it still has write mode and that would make the subsequent assertions to
    // fail because they expect it to have read-only mode. To prevent this we manually trigger
    // the garbage collector and wait for it to finish, to make sure the root dir is dropped and
    // removed from the cache. Then `open_file` reopens the root dir correctly in read-only mode.
    repo.force_garbage_collection().await.unwrap();

    let mut file = repo.open_file("test.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    assert_matches!(
        file.truncate(&mut tx, 0).await,
        Err(Error::PermissionDenied)
    );
    tx.commit().await.unwrap();

    let mut file = repo.open_file("test.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    file.write(&mut tx, b"bar").await.unwrap();
    assert_matches!(file.flush(&mut tx).await, Err(Error::PermissionDenied));
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_create_file() {
    let (_base_dir, repo) = setup().await;
    let local_branch = repo.local_branch().unwrap();

    let root_vv_0 = {
        let mut conn = repo.db().acquire().await.unwrap();
        local_branch.version_vector(&mut conn).await.unwrap()
    };

    let mut file = repo.create_file("parent/test.txt").await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();

    let root_vv_1 = file
        .parent(&mut tx)
        .await
        .unwrap()
        .parent(&mut tx)
        .await
        .unwrap()
        .unwrap()
        .version_vector(&mut tx)
        .await
        .unwrap();
    let parent_vv_1 = file
        .parent(&mut tx)
        .await
        .unwrap()
        .version_vector(&mut tx)
        .await
        .unwrap();
    let file_vv_1 = file.version_vector(&mut tx).await.unwrap();

    assert!(root_vv_1 > root_vv_0);

    file.write(&mut tx, b"blah").await.unwrap();
    file.flush(&mut tx).await.unwrap();

    let root_vv_2 = file
        .parent(&mut tx)
        .await
        .unwrap()
        .parent(&mut tx)
        .await
        .unwrap()
        .unwrap()
        .version_vector(&mut tx)
        .await
        .unwrap();
    let parent_vv_2 = file
        .parent(&mut tx)
        .await
        .unwrap()
        .version_vector(&mut tx)
        .await
        .unwrap();
    let file_vv_2 = file.version_vector(&mut tx).await.unwrap();

    assert!(root_vv_2 > root_vv_1);
    assert!(parent_vv_2 > parent_vv_1);
    assert!(file_vv_2 > file_vv_1);
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_deep_hierarchy() {
    let (_base_dir, repo) = setup().await;
    let local_branch = repo.local_branch().unwrap();
    let local_id = *local_branch.id();

    let depth = 10;
    let mut conn = repo.db().acquire().await.unwrap();
    let mut dirs = Vec::new();
    dirs.push(local_branch.open_or_create_root(&mut conn).await.unwrap());

    for i in 0..depth {
        let dir = dirs
            .last_mut()
            .unwrap()
            .create_directory(&mut conn, format!("dir-{}", i))
            .await
            .unwrap();
        dirs.push(dir);
    }

    // Each directory's local version is one less than its parent.
    for (index, dir) in dirs.iter().skip(1).enumerate() {
        assert_eq!(
            dir.version_vector(&mut conn).await.unwrap(),
            vv![local_id => (depth - index) as u64]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_recreate_deleted_file() {
    let (_base_dir, repo) = setup().await;

    let local_id = *repo.local_branch().unwrap().id();

    let file = repo.create_file("test.txt").await.unwrap();
    drop(file);

    repo.remove_entry("test.txt").await.unwrap();

    let file = repo.create_file("test.txt").await.unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    assert_eq!(
        file.version_vector(&mut conn).await.unwrap(),
        vv![local_id => 3 /* = 1*create + 1*remove + 1*create again */]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_fork() {
    let (_base_dir, repo) = setup().await;

    let local_branch = repo.local_branch().unwrap();

    let remote_branch = repo
        .get_branch(PublicKey::random())
        .unwrap()
        .reopen(repo.secrets().keys().unwrap());

    let mut conn = repo.db().acquire().await.unwrap();

    let mut remote_root = remote_branch.open_or_create_root(&mut conn).await.unwrap();
    let mut remote_parent = remote_root
        .create_directory(&mut conn, "parent".into())
        .await
        .unwrap();
    let mut file = create_file_in_directory(&mut conn, &mut remote_parent, "foo.txt", &[]).await;

    remote_parent.refresh(&mut conn).await.unwrap();
    let remote_parent_vv = remote_parent.version_vector(&mut conn).await.unwrap();
    let remote_file_vv = file.version_vector(&mut conn).await.unwrap();

    file.fork(&mut conn, local_branch.clone()).await.unwrap();

    let local_file_vv_0 = file.version_vector(&mut conn).await.unwrap();
    assert_eq!(local_file_vv_0, remote_file_vv);

    let local_parent_vv_0 = local_branch
        .open_root(&mut conn)
        .await
        .unwrap()
        .lookup("parent")
        .unwrap()
        .version_vector()
        .clone();

    assert!(local_parent_vv_0 <= remote_parent_vv);

    // modify the file and fork again
    let mut file = remote_parent
        .lookup("foo.txt")
        .unwrap()
        .file()
        .unwrap()
        .open(&mut conn)
        .await
        .unwrap();
    file.write(&mut conn, b"hello").await.unwrap();
    file.flush(&mut conn).await.unwrap();

    remote_parent.refresh(&mut conn).await.unwrap();
    let remote_parent_vv = remote_parent.version_vector(&mut conn).await.unwrap();
    let remote_file_vv = file.version_vector(&mut conn).await.unwrap();

    file.fork(&mut conn, local_branch.clone()).await.unwrap();

    let local_file_vv_1 = file.version_vector(&mut conn).await.unwrap();
    assert_eq!(local_file_vv_1, remote_file_vv);
    assert!(local_file_vv_1 > local_file_vv_0);

    let local_parent_vv_1 = local_branch
        .open_root(&mut conn)
        .await
        .unwrap()
        .lookup("parent")
        .unwrap()
        .version_vector()
        .clone();

    assert!(local_parent_vv_1 <= remote_parent_vv);
    assert!(local_parent_vv_1 > local_parent_vv_0);
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_empty_directory() {
    let (_base_dir, repo) = setup().await;

    let local_branch = repo.local_branch().unwrap();
    let local_id = *local_branch.id();

    let dir = repo.create_directory("stuff").await.unwrap();
    assert_eq!(
        dir.version_vector(&mut *repo.db().acquire().await.unwrap())
            .await
            .unwrap(),
        vv![local_id => 1]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_moved_non_empty_directory() {
    let (_base_dir, repo) = setup().await;

    repo.create_directory("foo").await.unwrap();
    repo.create_file("foo/stuff.txt").await.unwrap();

    let mut conn = repo.db().acquire().await.unwrap();
    let vv_0 = repo
        .local_branch()
        .unwrap()
        .open_root(&mut conn)
        .await
        .unwrap()
        .lookup("foo")
        .unwrap()
        .version_vector()
        .clone();

    repo.move_entry("/", "foo", "/", "bar").await.unwrap();

    let vv_1 = repo
        .local_branch()
        .unwrap()
        .open_root(&mut conn)
        .await
        .unwrap()
        .lookup("bar")
        .unwrap()
        .version_vector()
        .clone();

    assert!(vv_1 > vv_0);
}

#[tokio::test(flavor = "multi_thread")]
async fn version_vector_file_moved_over_tombstone() {
    let (_base_dir, repo) = setup().await;
    let mut conn = repo.db().acquire().await.unwrap();

    let mut file = repo.create_file("old.txt").await.unwrap();
    file.write(&mut conn, b"a").await.unwrap();
    file.flush(&mut conn).await.unwrap();

    let vv_0 = file.version_vector(&mut conn).await.unwrap();

    drop(file);
    repo.remove_entry("old.txt").await.unwrap();

    // The tombstone's vv is the original vv incremented by 1.
    let branch_id = *repo.local_branch().unwrap().id();
    let vv_1 = vv_0.incremented(branch_id);

    repo.create_file("new.txt").await.unwrap();
    repo.move_entry("/", "new.txt", "/", "old.txt")
        .await
        .unwrap();

    let vv_2 = repo
        .local_branch()
        .unwrap()
        .open_root(&mut conn)
        .await
        .unwrap()
        .lookup("old.txt")
        .unwrap()
        .version_vector()
        .clone();

    assert!(vv_2 > vv_1);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_conflict_modify_local() {
    let (_base_dir, repo) = setup().await;

    let local_branch = repo.local_branch().unwrap();
    let local_id = *local_branch.id();

    let remote_id = PublicKey::random();
    let remote_branch = repo
        .get_branch(remote_id)
        .unwrap()
        .reopen(repo.secrets().keys().unwrap());

    let mut conn = repo.db().acquire().await.unwrap();

    // Create two concurrent versions of the same file.
    let local_file = create_file_in_branch(&mut conn, &local_branch, "test.txt", b"local v1").await;
    assert_eq!(
        local_file.version_vector(&mut conn).await.unwrap(),
        vv![local_id => 2]
    );
    drop(local_file);

    let remote_file =
        create_file_in_branch(&mut conn, &remote_branch, "test.txt", b"remote v1").await;
    assert_eq!(
        remote_file.version_vector(&mut conn).await.unwrap(),
        vv![remote_id => 2]
    );
    drop(remote_file);

    drop(conn);

    repo.force_garbage_collection().await.unwrap();

    // Modify the local version.
    let mut local_file = repo.open_file_version("test.txt", &local_id).await.unwrap();
    let mut tx = repo.db().begin().await.unwrap();
    local_file.write(&mut tx, b"local v2").await.unwrap();
    local_file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    drop(local_file);

    let mut local_file = repo.open_file_version("test.txt", &local_id).await.unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    assert_eq!(
        local_file.read_to_end(&mut conn).await.unwrap(),
        b"local v2"
    );
    assert_eq!(
        local_file.version_vector(&mut conn).await.unwrap(),
        vv![local_id => 3]
    );
    drop(conn);

    let mut remote_file = repo
        .open_file_version("test.txt", &remote_id)
        .await
        .unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    assert_eq!(
        remote_file.read_to_end(&mut conn).await.unwrap(),
        b"remote v1"
    );
    assert_eq!(
        remote_file.version_vector(&mut conn).await.unwrap(),
        vv![remote_id => 2]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn file_conflict_attempt_to_fork_and_modify_remote() {
    let (_base_dir, repo) = setup().await;

    let local_branch = repo.local_branch().unwrap();

    let remote_id = PublicKey::random();
    let remote_branch = repo
        .get_branch(remote_id)
        .unwrap()
        .reopen(repo.secrets().keys().unwrap());

    // Create two concurrent versions of the same file.
    let mut conn = repo.db().acquire().await.unwrap();
    create_file_in_branch(&mut conn, &local_branch, "test.txt", b"local v1").await;
    create_file_in_branch(&mut conn, &remote_branch, "test.txt", b"remote v1").await;
    drop(conn);

    // Attempt to fork the remote version (fork is required to modify it)
    let mut remote_file = repo
        .open_file_version("test.txt", &remote_id)
        .await
        .unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    assert_matches!(
        remote_file.fork(&mut conn, local_branch).await,
        Err(Error::EntryExists)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_branch() {
    let (_base_dir, repo) = setup().await;

    let local_branch = repo.local_branch().unwrap();

    let remote_id = PublicKey::random();
    let remote_branch = repo
        .get_branch(remote_id)
        .unwrap()
        .reopen(repo.secrets().keys().unwrap());

    let mut conn = repo.db().acquire().await.unwrap();

    // Keep the root dir open until we create both files to make sure it keeps write access.
    let mut remote_root = remote_branch.open_or_create_root(&mut conn).await.unwrap();
    create_file_in_directory(&mut conn, &mut remote_root, "foo.txt", b"foo").await;
    create_file_in_directory(&mut conn, &mut remote_root, "bar.txt", b"bar").await;
    drop(remote_root);

    let mut file = repo.open_file("foo.txt").await.unwrap();
    file.fork(&mut conn, local_branch).await.unwrap();
    drop(file);

    repo.shared
        .store
        .index
        .get_branch(remote_id)
        .load_snapshot(&mut conn)
        .await
        .unwrap()
        .remove(&mut conn)
        .await
        .unwrap();

    // The forked file still exists
    assert_eq!(read_file(&repo, "foo.txt").await, b"foo");

    // The remote-only file is gone
    assert_matches!(repo.open_file("bar.txt").await, Err(Error::EntryNotFound));
}

async fn setup() -> (TempDir, Repository) {
    let base_dir = TempDir::new().unwrap();
    let repo = Repository::create(
        base_dir.path().join("repo.db"),
        rand::random(),
        MasterSecret::random(),
        AccessSecrets::random_write(),
        false,
    )
    .await
    .unwrap();

    (base_dir, repo)
}

async fn read_file(repo: &Repository, path: impl AsRef<Utf8Path>) -> Vec<u8> {
    let mut file = repo.open_file(path).await.unwrap();
    let mut conn = repo.db().acquire().await.unwrap();
    file.read_to_end(&mut conn).await.unwrap()
}

async fn create_remote_file(repo: &Repository, remote_id: PublicKey, name: &str, content: &[u8]) {
    let remote_branch = repo
        .get_branch(remote_id)
        .unwrap()
        .reopen(repo.secrets().keys().unwrap());

    let mut conn = repo.db().acquire().await.unwrap();

    create_file_in_branch(&mut conn, &remote_branch, name, content).await;
}

async fn create_file_in_branch(
    conn: &mut db::Connection,
    branch: &Branch,
    name: &str,
    content: &[u8],
) -> File {
    let mut root = branch.open_or_create_root(conn).await.unwrap();
    create_file_in_directory(conn, &mut root, name, content).await
}

async fn create_file_in_directory(
    conn: &mut db::Connection,
    dir: &mut Directory,
    name: &str,
    content: &[u8],
) -> File {
    let mut file = dir.create_file(conn, name.into()).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    file.write(&mut tx, content).await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    file
}

fn random_bytes(size: usize) -> Vec<u8> {
    let mut buffer = vec![0; size];
    rand::thread_rng().fill(&mut buffer[..]);
    buffer
}

fn nth_block(content: &[u8], index: usize) -> &[u8] {
    if index == 0 {
        &content[..BLOCK_SIZE - blob::HEADER_SIZE]
    } else {
        &content[BLOCK_SIZE - blob::HEADER_SIZE + (index - 1) * BLOCK_SIZE..][..BLOCK_SIZE]
    }
}

fn concat<const N: usize>(buffers: [&[u8]; N]) -> Vec<u8> {
    buffers.into_iter().fold(Vec::new(), |mut vec, buffer| {
        vec.extend_from_slice(buffer);
        vec
    })
}
