use super::lock::OpenLock;
use crate::{blob_id::BlobId, collections::HashMap, crypto::sign::PublicKey, event::Event};
use std::sync::{Arc, Mutex as BlockingMutex, Weak};
use tokio::sync::broadcast;

pub(crate) struct FileCache {
    slots: BlockingMutex<BranchMap>,
    event_tx: broadcast::Sender<Event>,
}

type FileMap = HashMap<BlobId, Weak<OpenLock>>;
type BranchMap = HashMap<PublicKey, FileMap>;

impl FileCache {
    pub fn new(event_tx: broadcast::Sender<Event>) -> Self {
        Self {
            slots: BlockingMutex::new(HashMap::default()),
            event_tx,
        }
    }

    pub fn acquire(&self, branch_id: PublicKey, blob_id: BlobId) -> Arc<OpenLock> {
        let mut slots = self.slots.lock().unwrap();

        // Cleanup
        for branch in slots.values_mut() {
            branch.retain(|_, slot| slot.strong_count() > 0);
        }

        slots.retain(|_, branch| !branch.is_empty());

        let slot = slots
            .entry(branch_id)
            .or_default()
            .entry(blob_id)
            .or_insert_with(Weak::new);

        if let Some(lock) = slot.upgrade() {
            lock
        } else {
            let lock = OpenLock::new(self.event_tx.clone());
            *slot = Arc::downgrade(&lock);
            lock
        }
    }
}
