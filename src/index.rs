use crate::{
    db,
    error::{Error, Result},
};

/// Initializes the index. Creates the required database schema unless already exists.
pub async fn init(pool: &db::Pool) -> Result<(), Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS branches (
             id                 INTEGER PRIMARY KEY,
             replica_id         BLOB NOT NULL,
             root_block_name    BLOB,
             root_block_version BLOB,
             merkle_root        BLOB NOT NULL
         );
         CREATE TABLE IF NOT EXISTS merkle_forest (
             /* Parent is a hash calculated from its children */
             parent  BLOB NOT NULL,
             bucket  INTEGER,
             /*
              * Node is a hash calculated from its children (as the `parent` is), or - if this is
              * a leaf layer - node is a blob serialized from the locator hash and BlockId
              */
             node BLOB NOT NULL
         );",
    )
    .execute(pool)
    .await
    .map_err(Error::CreateDbSchema)?;

    Ok(())
}
