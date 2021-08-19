use crate::{
    crypto::Cryptor,
    db,
    directory::{Directory, RootDirectoryCache},
    error::{Error, Result},
    file::File,
    index::BranchData,
    path, ReplicaId,
};
use camino::{Utf8Component, Utf8Path};
use std::sync::Arc;

#[derive(Clone)]
pub struct Branch {
    pool: db::Pool,
    branch_data: Arc<BranchData>,
    cryptor: Cryptor,
    root_directory: Arc<RootDirectoryCache>,
}

impl Branch {
    pub fn new(pool: db::Pool, branch_data: Arc<BranchData>, cryptor: Cryptor) -> Self {
        Self {
            pool,
            branch_data,
            cryptor,
            root_directory: Arc::new(RootDirectoryCache::new()),
        }
    }

    pub fn id(&self) -> &ReplicaId {
        self.branch_data.id()
    }

    pub fn data(&self) -> &BranchData {
        &self.branch_data
    }

    pub fn db_pool(&self) -> &db::Pool {
        &self.pool
    }

    pub fn cryptor(&self) -> &Cryptor {
        &self.cryptor
    }

    pub async fn open_root(&self, local_branch: Branch) -> Result<Directory> {
        self.root_directory.open(self.clone(), local_branch).await
    }

    pub async fn open_or_create_root(&self) -> Result<Directory> {
        self.root_directory.open_or_create(self.clone()).await
    }

    /// Ensures that the directory at the specified path exists including all its ancestors.
    /// Note: non-normalized paths (i.e. containing "..") or Windows-style drive prefixes
    /// (e.g. "C:") are not supported.
    pub async fn ensure_directory_exists(&self, path: &Utf8Path) -> Result<Directory> {
        let mut curr = self.open_or_create_root().await?;

        for component in path.components() {
            match component {
                Utf8Component::RootDir | Utf8Component::CurDir => (),
                Utf8Component::Normal(name) => {
                    let next = if let Ok(entry) = curr.read().await.lookup_version(name, self.id())
                    {
                        entry.directory()?.open().await?
                    } else {
                        curr.create_directory(name.to_string()).await?
                    };

                    curr = next;
                }
                Utf8Component::Prefix(_) | Utf8Component::ParentDir => {
                    return Err(Error::OperationNotSupported)
                }
            }
        }

        Ok(curr)
    }

    pub async fn ensure_file_exists(&self, path: &Utf8Path) -> Result<File> {
        let (parent, name) = path::decompose(path).ok_or(Error::EntryIsDirectory)?;
        let dir = self.ensure_directory_exists(parent).await?;
        dir.create_file(name.to_string()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db, index::Index, locator::Locator};

    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_root_directory_exists() {
        let pool = db::init(db::Store::Memory).await.unwrap();
        let replica_id = rand::random();
        let index = Index::load(pool.clone(), replica_id).await.unwrap();
        let branch = Branch::new(pool, index.branches().await.local().clone(), Cryptor::Null);

        let dir = branch.ensure_directory_exists("/".into()).await.unwrap();
        assert_eq!(dir.read().await.locator(), &Locator::Root);
    }
}
