use crate::{
    blob::{self, Blob},
    branch::Branch,
    db,
    directory::{Directory, ParentContext},
    error::Result,
    locator::Locator,
    version_vector::VersionVector,
};
use std::io::SeekFrom;
use std::{fmt, sync::Arc};
use tokio::sync::Mutex;

pub struct File {
    blob: Blob,
    parent: ParentContext,
    pool: db::Pool,
}

impl File {
    /// Opens an existing file.
    pub(crate) async fn open(
        owner_branch: Branch,
        locator: Locator,
        parent: ParentContext,
    ) -> Result<Self> {
        let pool = owner_branch.db_pool().clone();

        Ok(Self {
            blob: Blob::open(owner_branch, locator).await?,
            parent,
            pool,
        })
    }

    /// Opens an existing file. Reuse the already opened blob::Core
    pub(crate) async fn reopen(
        blob_core: Arc<Mutex<blob::Core>>,
        pool: db::Pool,
        parent: ParentContext,
    ) -> Result<Self> {
        Ok(Self {
            blob: Blob::reopen(blob_core).await?,
            parent,
            pool,
        })
    }

    /// Creates a new file.
    pub(crate) fn create(branch: Branch, locator: Locator, parent: ParentContext) -> Self {
        Self {
            blob: Blob::create(branch.clone(), locator),
            parent,
            pool: branch.db_pool().clone(),
        }
    }

    pub fn branch(&self) -> &Branch {
        self.blob.branch()
    }

    pub fn parent(&self) -> Directory {
        self.parent.directory(self.pool.clone())
    }

    /// Length of this file in bytes.
    #[allow(clippy::len_without_is_empty)]
    pub async fn len(&self) -> u64 {
        self.blob.len().await
    }

    /// Reads data from this file. See [`Blob::read`] for more info.
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        self.blob.read(buffer).await
    }

    /// Read all data from this file from the current seek position until the end and return then
    /// in a `Vec`.
    pub async fn read_to_end(&mut self) -> Result<Vec<u8>> {
        self.blob.read_to_end().await
    }

    /// Writes `buffer` into this file.
    pub async fn write(&mut self, buffer: &[u8], local_branch: &Branch) -> Result<()> {
        self.fork(local_branch).await?;
        self.blob.write(buffer).await
    }

    /// Seeks to an offset in the file.
    pub async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.blob.seek(pos).await
    }

    /// Truncates the file to the given length.
    pub async fn truncate(&mut self, len: u64, local_branch: &Branch) -> Result<()> {
        self.fork(local_branch).await?;
        self.blob.truncate(len).await
    }

    /// Flushes this file, ensuring that all intermediately buffered contents gets written to the
    /// store.
    pub async fn flush(&mut self) -> Result<()> {
        if !self.blob.is_dirty().await {
            return Ok(());
        }

        let mut tx = self.blob.db_pool().begin().await?;

        self.blob.flush_in_transaction(&mut tx).await?;

        // Since the blob is dirty, it must be that it's been forked onto the local branch. That in
        // turn means that self.blob.branch().id() is the ID of the local writer.
        let local_writer_id = self.blob.branch().id();

        self.parent.modify_entry(tx, local_writer_id, None).await?;

        Ok(())
    }

    pub(crate) fn blob_core(&self) -> &Arc<Mutex<blob::Core>> {
        self.blob.core()
    }

    /// Forks this file into the local branch. Ensure all its ancestor directories exist and live
    /// in the local branch as well. Should be called before any mutable operation.
    pub async fn fork(&mut self, local_branch: &Branch) -> Result<()> {
        if self.blob.branch().id() == local_branch.id() {
            // File already lives in the local branch. We assume the ancestor directories have been
            // already created as well so there is nothing else to do.
            return Ok(());
        }

        // TODO: this should be atomic
        let blob_id = self.parent.fork_file(local_branch).await?;
        self.blob
            .fork(local_branch.clone(), Locator::head(blob_id))
            .await
    }

    pub async fn version_vector(&self) -> VersionVector {
        self.parent.entry_version_vector().await
    }

    /// Locator of this file.
    #[cfg(test)]
    pub(crate) fn locator(&self) -> &Locator {
        self.blob.locator()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{crypto::Cryptor, db, index::BranchData, repository};
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn fork() {
        let branch0 = setup().await;
        let branch1 = create_branch(branch0.db_pool().clone()).await;

        // Create a file owned by branch 0
        let mut file0 = branch0.ensure_file_exists("/dog.jpg".into()).await.unwrap();

        file0.write(b"small", &branch0).await.unwrap();
        file0.flush().await.unwrap();

        // Write to the file by branch 1
        let mut file1 = branch0
            .open_root()
            .await
            .unwrap()
            .read()
            .await
            .lookup_version("dog.jpg", branch0.id())
            .unwrap()
            .file()
            .unwrap()
            .open()
            .await
            .unwrap();

        // This will create a fork on branch 1
        file1.write(b"large", &branch1).await.unwrap();
        file1.flush().await.unwrap();

        // Reopen orig file and verify it's unchanged
        let mut file = branch0
            .open_root()
            .await
            .unwrap()
            .read()
            .await
            .lookup_version("dog.jpg", branch0.id())
            .unwrap()
            .file()
            .unwrap()
            .open()
            .await
            .unwrap();

        assert_eq!(file.read_to_end().await.unwrap(), b"small");

        // Reopen forked file and verify it's modified
        let mut file = branch1
            .open_root()
            .await
            .unwrap()
            .read()
            .await
            .lookup_version("dog.jpg", branch1.id())
            .unwrap()
            .file()
            .unwrap()
            .open()
            .await
            .unwrap();

        assert_eq!(file.read_to_end().await.unwrap(), b"large");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_consecutive_modifications_of_forked_file() {
        // This test makes sure that modifying a forked file properly updates the file metadata so
        // subsequent modifications work correclty.

        let branch0 = setup().await;
        let branch1 = create_branch(branch0.db_pool().clone()).await;

        let mut file0 = branch0.ensure_file_exists("/pig.jpg".into()).await.unwrap();
        file0.flush().await.unwrap();

        let mut file1 = branch0
            .open_root()
            .await
            .unwrap()
            .read()
            .await
            .lookup_version("pig.jpg", branch0.id())
            .unwrap()
            .file()
            .unwrap()
            .open()
            .await
            .unwrap();

        file1.fork(&branch1).await.unwrap();

        for _ in 0..2 {
            file1.write(b"oink", &branch1).await.unwrap();
            file1.flush().await.unwrap();
        }
    }

    async fn setup() -> Branch {
        let pool = repository::create_db(&db::Store::Memory).await.unwrap();
        create_branch(pool).await
    }

    async fn create_branch(pool: db::Pool) -> Branch {
        let (notify_tx, _) = async_broadcast::broadcast(1);
        let branch_data = BranchData::new(&pool, rand::random(), notify_tx)
            .await
            .unwrap();
        Branch::new(pool, Arc::new(branch_data), Cryptor::Null)
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("File")
            .field("blob_id", &self.blob.blob_id())
            .field("branch", &self.blob.branch().id())
            .finish()
    }
}
