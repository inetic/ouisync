use super::Shared;
use crate::{
    branch::Branch,
    directory::MissingBlockStrategy,
    error::{Error, Result},
    event::Payload,
    event::{EventScope, IgnoreScopeReceiver},
    joint_directory::JointDirectory,
};
use std::sync::Arc;
use tokio::{
    select,
    sync::{broadcast::error::RecvError, mpsc, oneshot},
};
use tracing::instrument;

/// Background worker to perform various jobs on the repository:
/// - merge remote branches into the local one
/// - remove outdated branches and snapshots
/// - remove unreachable blocks
/// - find missing blocks
pub(super) struct Worker {
    inner: Inner,
    command_rx: mpsc::Receiver<Command>,
    abort_rx: oneshot::Receiver<()>,
}

impl Worker {
    pub fn new(shared: Arc<Shared>, local_branch: Option<Branch>) -> (Self, WorkerHandle) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (abort_tx, abort_rx) = oneshot::channel();

        let inner = Inner {
            shared,
            local_branch,
            event_scope: EventScope::new(),
        };

        let worker = Self {
            inner,
            command_rx,
            abort_rx,
        };
        let handle = WorkerHandle {
            command_tx,
            _abort_tx: abort_tx,
        };

        (worker, handle)
    }

    pub async fn run(self) {
        select! {
            _ = self.inner.run(self.command_rx) => (),
            _ = self.abort_rx => (),
        }
    }
}

/// Handle to interact with the worker. Aborts the worker task when dropped.
pub(super) struct WorkerHandle {
    command_tx: mpsc::Sender<Command>,
    _abort_tx: oneshot::Sender<()>,
}

impl WorkerHandle {
    pub async fn work(&self) -> Result<()> {
        self.oneshot(Command::Work).await
    }

    pub async fn shutdown(&self) {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Shutdown(result_tx))
            .await
            .unwrap_or(());
        result_rx.await.unwrap_or(())
    }

    async fn oneshot<F>(&self, command_fn: F) -> Result<()>
    where
        F: FnOnce(oneshot::Sender<Result<()>>) -> Command,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(command_fn(result_tx))
            .await
            .unwrap_or(());

        // When this returns error it means the worker has been terminated which we treat as
        // success, for simplicity.
        result_rx.await.unwrap_or(Ok(()))
    }
}

enum Command {
    Work(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<()>),
}

struct Inner {
    shared: Arc<Shared>,
    local_branch: Option<Branch>,
    event_scope: EventScope,
}

impl Inner {
    async fn run(self, mut command_rx: mpsc::Receiver<Command>) {
        let mut event_rx =
            IgnoreScopeReceiver::new(self.shared.store.index.subscribe(), self.event_scope);

        enum State {
            Working,
            Waiting,
            Terminated,
        }

        let mut state = State::Working;

        loop {
            match state {
                State::Working => {
                    state = State::Waiting;

                    let work = async {
                        self.work(ErrorHandling::Ignore).await.ok();
                    };

                    let wait = async {
                        loop {
                            let event = event_rx.recv().await;
                            tracing::trace!(?event);

                            match event {
                                Ok(Payload::BranchChanged(_)) => {
                                    // On `BranchChanged`, interrupt the current job and
                                    // immediately start a new one.
                                    tracing::trace!("job interrupted");
                                    state = State::Working;
                                    break;
                                }
                                Ok(Payload::BlockReceived { .. }) | Err(RecvError::Lagged(_)) => {
                                    // On any other event, let the current job run to completion
                                    // and then start a new one.
                                    state = State::Working;
                                }
                                Err(RecvError::Closed) => {
                                    state = State::Terminated;
                                    break;
                                }
                            }
                        }
                    };

                    select! {
                        _ = work => (),
                        _ = wait => (),
                    }
                }
                State::Waiting => {
                    state = select! {
                        event = event_rx.recv() => {
                            tracing::trace!(?event);

                            match event {
                                Ok(_) | Err(RecvError::Lagged(_)) => State::Working,
                                Err(RecvError::Closed) => State::Terminated,
                            }
                        }
                        command = command_rx.recv() => {
                            if let Some(command) = command {
                                if self.handle_command(command).await {
                                    State::Waiting
                                } else {
                                    State::Terminated
                                }
                            } else {
                                State::Terminated
                            }
                        }
                    }
                }
                State::Terminated => break,
            }
        }
    }

    async fn handle_command(&self, command: Command) -> bool {
        match command {
            Command::Work(result_tx) => {
                result_tx
                    .send(self.work(ErrorHandling::Return).await)
                    .unwrap_or(());
                true
            }
            Command::Shutdown(result_tx) => {
                result_tx.send(()).unwrap_or(());
                false
            }
        }
    }

    async fn work(&self, error_handling: ErrorHandling) -> Result<()> {
        tracing::trace!("job started");

        // Merge
        if let Some(local_branch) = &self.local_branch {
            let result = self
                .event_scope
                .apply(merge::run(&self.shared, local_branch))
                .await;
            tracing::trace!(?result, "merge completed");

            error_handling.apply(result)?;
        }

        // Prune outdated branches and snapshots
        let result = prune::run(&self.shared).await;
        tracing::trace!(?result, "prune completed");

        error_handling.apply(result)?;

        // Scan for missing and unreachable blocks
        if self.shared.secrets.can_read() {
            let result = scan::run(&self.shared).await;
            tracing::trace!(?result, "scan completed");

            error_handling.apply(result)?;
        }

        tracing::trace!("job completed");

        Ok(())
    }
}

#[derive(Copy, Clone)]
enum ErrorHandling {
    Return,
    Ignore,
}

impl ErrorHandling {
    fn apply(self, result: Result<()>) -> Result<()> {
        match (self, result) {
            (_, Ok(())) | (Self::Ignore, Err(_)) => Ok(()),
            (Self::Return, Err(error)) => Err(error),
        }
    }
}

/// Merge remote branches into the local one.
mod merge {
    use super::*;
    use crate::{
        index::SnapshotData,
        joint_directory::versioned::{self, TiebreakStrategy},
    };

    #[instrument(name = "merge", skip_all, err(Debug))]
    pub(super) async fn run(shared: &Shared, local_branch: &Branch) -> Result<()> {
        let snapshots = shared.load_snapshots().await?;
        let snapshots = versioned::keep_maximal(snapshots, TiebreakStrategy::Global);

        // If there is only one branch which is newer than all the other branches, then fork it
        // into the local branch (unless it's already the local one) and we are done.
        if snapshots.len() <= 1 {
            if let Some(snapshot) = snapshots.first() {
                fork(shared, snapshot).await?;
                return Ok(());
            }
        }

        // Otherwise we need to proceed with deep merge.
        let branches = inflate_all(shared, snapshots)?;
        let mut roots = Vec::with_capacity(branches.len());

        for branch in branches {
            match branch.open_root(MissingBlockStrategy::Fail).await {
                Ok(dir) => roots.push(dir),
                Err(Error::EntryNotFound | Error::BlockNotFound(_)) => continue,
                Err(error) => return Err(error),
            }
        }

        match JointDirectory::new(Some(local_branch.clone()), roots)
            .merge()
            .await
        {
            Ok(_) | Err(Error::AmbiguousEntry) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn fork(shared: &Shared, snapshot: &SnapshotData) -> Result<()> {
        if snapshot.branch_id() == &shared.this_writer_id {
            return Ok(());
        }

        let write_keys = shared
            .secrets
            .write_secrets()
            .map(|secrets| &secrets.write_keys)
            .ok_or(Error::PermissionDenied)?;

        let mut tx = shared.store.db().begin_write().await?;
        snapshot
            .fork(&mut tx, shared.this_writer_id, write_keys)
            .await?;
        tx.commit().await?;

        Ok(())
    }

    fn inflate_all(shared: &Shared, snapshots: Vec<SnapshotData>) -> Result<Vec<Branch>> {
        snapshots
            .into_iter()
            .map(|snapshot| snapshot.to_branch_data())
            .map(|data| shared.inflate(data))
            .collect()
    }
}

/// Remove outdated branches and snapshots.
mod prune {
    use super::*;
    use crate::joint_directory::versioned::{self, TiebreakStrategy};

    #[instrument(name = "prune", skip_all, err(Debug))]
    pub(super) async fn run(shared: &Shared) -> Result<()> {
        let all = shared.store.index.load_snapshots().await?;
        let (uptodate, outdated): (Vec<_>, Vec<_>) =
            versioned::partition(all, TiebreakStrategy::Local(&shared.this_writer_id));

        // Remove outdated branches
        for snapshot in outdated {
            // Never remove local branch
            if snapshot.branch_id() == &shared.this_writer_id {
                continue;
            }

            let _guard = if let Some(guard) = shared
                .branch_shared
                .branch_pinner
                .prune(*snapshot.branch_id())
            {
                guard
            } else {
                tracing::trace!(id = ?snapshot.branch_id(), "outdated branch not removed - in use");
                continue;
            };

            let mut tx = shared.store.db().begin_write().await?;
            snapshot.remove_all_older(&mut tx).await?;
            snapshot.remove(&mut tx).await?;
            tx.commit().await?;

            tracing::trace!(
                branch_id = ?snapshot.branch_id(),
                vv = ?snapshot.version_vector(),
                hash = ?snapshot.root_hash(),
                "outdated branch removed"
            );
        }

        // Remove outdated snapshots.
        for snapshot in uptodate {
            snapshot.prune(shared.store.db()).await?;
        }

        Ok(())
    }
}

/// Scan repository blocks to identify which ones are missing and which are no longer needed.
mod scan {
    use super::*;
    use crate::{
        blob::BlockIds,
        blob_id::BlobId,
        block::{self, BlockId},
        crypto::sign::Keypair,
        db,
        index::{self, LeafNode, SnapshotData},
        joint_directory::{JointEntryRef, MissingVersionStrategy},
    };
    use async_recursion::async_recursion;
    use futures_util::TryStreamExt;
    use std::collections::BTreeSet;
    use tracing::Instrument;

    #[derive(Copy, Clone, Debug)]
    enum Mode {
        // only require missing blocks
        Require,
        // only collect unreachable blocks
        Collect,
        // require missing blocks and collect unreachable blocks
        RequireAndCollect,
    }

    impl Mode {
        fn intersect(self, other: Self) -> Option<Self> {
            match (self, other) {
                (Self::Require, Self::Require)
                | (Self::Require, Self::RequireAndCollect)
                | (Self::RequireAndCollect, Self::Require) => Some(Self::Require),
                (Self::Collect, Self::Collect)
                | (Self::Collect, Self::RequireAndCollect)
                | (Self::RequireAndCollect, Self::Collect) => Some(Self::Collect),
                (Self::RequireAndCollect, Self::RequireAndCollect) => Some(Self::RequireAndCollect),
                (Self::Require, Self::Collect) | (Self::Collect, Self::Require) => None,
            }
        }
    }

    #[instrument(name = "scan", skip(shared), err(Debug))]
    pub(super) async fn run(shared: &Shared) -> Result<()> {
        // Perform the scan in multiple passes, to avoid loading too many block ids into memory.
        // The first pass is used both for requiring missing blocks and collecting unreachable
        // blocks. The subsequent passes (if any) for collecting only.
        const UNREACHABLE_BLOCKS_PAGE_SIZE: u32 = 1_000_000;

        let mut mode = Mode::RequireAndCollect;
        let mut unreachable_block_ids_page = shared.store.block_ids(UNREACHABLE_BLOCKS_PAGE_SIZE);

        loop {
            let mut unreachable_block_ids = unreachable_block_ids_page.next().await?;
            if unreachable_block_ids.is_empty() {
                break;
            }

            process_pinned_blocks(shared, &mut unreachable_block_ids).await?;

            mode = traverse_root(shared, mode, &mut unreachable_block_ids).await?;
            mode = if let Some(mode) = mode.intersect(Mode::Collect) {
                mode
            } else {
                break;
            };

            remove_unreachable_blocks(shared, unreachable_block_ids).await?;
        }

        Ok(())
    }

    async fn traverse_root(
        shared: &Shared,
        mut mode: Mode,
        unreachable_block_ids: &mut BTreeSet<BlockId>,
    ) -> Result<Mode> {
        let branches = shared.load_branches().await?;

        let mut versions = Vec::with_capacity(branches.len());
        let mut entries = Vec::new();

        for branch in branches {
            entries.push((branch.clone(), BlobId::ROOT));

            match branch.open_root(MissingBlockStrategy::Fail).await {
                Ok(dir) => versions.push(dir),
                Err(Error::EntryNotFound) => {
                    // `EntryNotFound` here just means this is a newly created branch with no
                    // content yet. It is safe to ignore it.
                    continue;
                }
                Err(error) => {
                    // On error, we can still proceed with finding missing blocks, but we can't
                    // proceed with garbage collection because we could incorrectly mark some
                    // blocks as unreachable due to not being able to descend into this directory.
                    mode = mode.intersect(Mode::Require).ok_or(error)?;
                }
            }
        }

        for (branch, blob_id) in entries {
            process_reachable_blocks(shared, mode, unreachable_block_ids, branch, blob_id).await?;
        }

        let local_branch = shared.local_branch().ok();
        traverse(
            shared,
            mode,
            unreachable_block_ids,
            JointDirectory::new(local_branch, versions),
        )
        .await
    }

    #[async_recursion]
    async fn traverse(
        shared: &Shared,
        mut mode: Mode,
        unreachable_block_ids: &mut BTreeSet<BlockId>,
        dir: JointDirectory,
    ) -> Result<Mode> {
        let mut entries = Vec::new();
        let mut subdirs = Vec::new();

        // Collect the entries first, so we don't keep the directories locked while we are
        // processing the entries.
        for entry in dir.entries() {
            match entry {
                JointEntryRef::File(entry) => {
                    entries.push((entry.inner().branch().clone(), *entry.inner().blob_id()));
                }
                JointEntryRef::Directory(entry) => {
                    for version in entry.versions() {
                        entries.push((version.branch().clone(), *version.blob_id()));
                    }

                    match entry
                        .open_with(MissingVersionStrategy::Fail, MissingBlockStrategy::Fail)
                        .await
                    {
                        Ok(dir) => subdirs.push(dir),
                        Err(error) => {
                            // On error, we can still proceed with finding missing blocks, but we
                            // can't proceed with garbage collection because we could incorrectly
                            // mark some blocks as unreachable due to not being able to descend
                            // into this directory.
                            mode = mode.intersect(Mode::Require).ok_or(error)?;
                        }
                    }
                }
            }
        }

        for (branch, blob_id) in entries {
            process_reachable_blocks(shared, mode, unreachable_block_ids, branch, blob_id).await?;
        }

        for dir in subdirs {
            mode = traverse(shared, mode, unreachable_block_ids, dir).await?;
        }

        Ok(mode)
    }

    async fn process_reachable_blocks(
        shared: &Shared,
        mode: Mode,
        unreachable_block_ids: &mut BTreeSet<BlockId>,
        branch: Branch,
        blob_id: BlobId,
    ) -> Result<()> {
        let mut blob_block_ids = BlockIds::open(branch, blob_id).await?;

        while let Some(block_id) = blob_block_ids.try_next().await? {
            if matches!(mode, Mode::RequireAndCollect | Mode::Collect) {
                unreachable_block_ids.remove(&block_id);
            }

            if matches!(mode, Mode::RequireAndCollect | Mode::Require) {
                shared.store.require_missing_block(block_id).await?;
            }
        }

        Ok(())
    }

    /// Remove blocks of pinned blobs from the `unreachable_block_ids` set.
    async fn process_pinned_blocks(
        shared: &Shared,
        unreachable_block_ids: &mut BTreeSet<BlockId>,
    ) -> Result<()> {
        let pins = shared.branch_shared.blob_collect_pinner.all();
        if pins.is_empty() {
            return Ok(());
        }

        for (branch_id, blob_ids) in pins {
            let Ok(branch) = shared.get_branch(branch_id) else { continue };

            for blob_id in blob_ids {
                let mut blob_block_ids = BlockIds::open(branch.clone(), blob_id).await?;

                while let Some(block_id) = blob_block_ids.try_next().await? {
                    unreachable_block_ids.remove(&block_id);
                }
            }
        }

        Ok(())
    }

    async fn remove_unreachable_blocks(
        shared: &Shared,
        unreachable_block_ids: BTreeSet<BlockId>,
    ) -> Result<()> {
        // We need to delete the blocks and also mark them as missing (so they can be requested in
        // case they become needed again) in their corresponding leaf nodes and then update the
        // summaries of the corresponding ancestor nodes. This is a complex and potentially
        // expensive operation which is why we do it a few blocks at a time.
        const BATCH_SIZE: usize = 32;

        let mut unreachable_block_ids = unreachable_block_ids.into_iter();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut total_count = 0;

        let local_branch = shared.local_branch().ok();
        let local_branch_and_write_keys = local_branch
            .as_ref()
            .and_then(|branch| branch.keys().write().map(|keys| (branch, keys)));

        loop {
            batch.clear();
            batch.extend(unreachable_block_ids.by_ref().take(BATCH_SIZE));

            if batch.is_empty() {
                break;
            }

            let mut tx = shared.store.db().begin_write().await?;

            total_count += batch.len();

            if let Some((local_branch, write_keys)) = &local_branch_and_write_keys {
                let mut snapshot = local_branch.data().load_snapshot(&mut tx).await?;
                remove_local_nodes(&mut tx, &mut snapshot, write_keys, &batch).await?;
            }

            remove_blocks(&mut tx, &batch).await?;

            tx.commit().await?;

            // If we modified the local branch (by removing nodes from it), we need to notify, to
            // let other replicas know about the change.
            if let Some((local_branch, _)) = &local_branch_and_write_keys {
                local_branch.data().notify();
            }
        }

        if total_count > 0 {
            tracing::debug!("unreachable blocks removed: {}", total_count);
        }

        Ok(())
    }

    async fn remove_local_nodes(
        tx: &mut db::WriteTransaction,
        snapshot: &mut SnapshotData,
        write_keys: &Keypair,
        block_ids: &[BlockId],
    ) -> Result<()> {
        for block_id in block_ids {
            let locators: Vec<_> = LeafNode::load_locators(tx, block_id).try_collect().await?;
            let span = tracing::info_span!("remove_local_node", ?block_id);

            for locator in locators {
                match snapshot
                    .remove_block(tx, &locator, Some(block_id), write_keys)
                    .instrument(span.clone())
                    .await
                {
                    Ok(()) | Err(Error::EntryNotFound) => (),
                    Err(error) => return Err(error),
                }
            }
        }

        Ok(())
    }

    async fn remove_blocks(tx: &mut db::WriteTransaction, block_ids: &[BlockId]) -> Result<()> {
        for block_id in block_ids {
            tracing::trace!(?block_id, "unreachable block removed");

            block::remove(tx, block_id).await?;

            LeafNode::set_missing(tx, block_id).await?;

            let parent_hashes: Vec<_> = LeafNode::load_parent_hashes(tx, block_id)
                .try_collect()
                .await?;

            for hash in parent_hashes {
                index::update_summaries(tx, hash).await?;
            }
        }

        Ok(())
    }
}
