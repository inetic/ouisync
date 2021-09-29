use super::{
    session,
    utils::{self, Port, SharedHandle, UniqueHandle},
};
use crate::{error::Error, joint_entry::JointEntryType, path, repository::Repository};
use std::os::raw::c_char;
use tokio::task::JoinHandle;

pub const ENTRY_TYPE_INVALID: u8 = 0;
pub const ENTRY_TYPE_FILE: u8 = 1;
pub const ENTRY_TYPE_DIRECTORY: u8 = 2;

/// Opens a repository.
#[no_mangle]
pub unsafe extern "C" fn repository_open(
    _name: *const c_char,
    port: Port<SharedHandle<Repository>>,
    error: *mut *mut c_char,
) {
    session::with(port, error, |_ctx| {
        todo!()
        // let name = utils::ptr_to_str(name)?;
        // let repos = ctx.repositories().clone();

        // ctx.spawn(async move {
        //     let repo = repos
        //         .read()
        //         .await
        //         .get(name)
        //         .ok_or(Error::EntryNotFound)?
        //         .clone();
        //     let repo = Arc::new(repo);
        //     Ok(SharedHandle::new(repo))
        // })
    })
}

/// Closes a repository.
#[no_mangle]
pub unsafe extern "C" fn repository_close(handle: SharedHandle<Repository>) {
    handle.release();
}

/// Returns the type of repository entry (file, directory, ...).
/// If the entry doesn't exists, returns `ENTRY_TYPE_INVALID`, not an error.
#[no_mangle]
pub unsafe extern "C" fn repository_entry_type(
    handle: SharedHandle<Repository>,
    path: *const c_char,
    port: Port<u8>,
    error: *mut *mut c_char,
) {
    session::with(port, error, |ctx| {
        let repo = handle.get();
        let path = utils::ptr_to_path_buf(path)?;

        ctx.spawn(async move {
            match repo.lookup_type(path).await {
                Ok(entry_type) => Ok(entry_type_to_num(entry_type)),
                Err(Error::EntryNotFound) => Ok(ENTRY_TYPE_INVALID),
                Err(error) => Err(error),
            }
        })
    })
}

/// Move/rename entry from src to dst.
#[no_mangle]
pub unsafe extern "C" fn repository_move_entry(
    handle: SharedHandle<Repository>,
    src: *const c_char,
    dst: *const c_char,
    port: Port<()>,
    error: *mut *mut c_char,
) {
    session::with(port, error, |ctx| {
        let repo = handle.get();
        let src = utils::ptr_to_path_buf(src)?;
        let dst = utils::ptr_to_path_buf(dst)?;

        ctx.spawn(async move {
            let (src_dir, src_name) = path::utf8::decompose(&src).ok_or(Error::EntryNotFound)?;
            let (dst_dir, dst_name) = path::utf8::decompose(&dst).ok_or(Error::EntryNotFound)?;

            repo.move_entry(src_dir, src_name, dst_dir, dst_name).await
        })
    })
}

/// Subscribe to change notifications from the repository.
#[no_mangle]
pub unsafe extern "C" fn repository_subscribe(
    handle: SharedHandle<Repository>,
    port: Port<()>,
) -> UniqueHandle<JoinHandle<()>> {
    let session = session::get();
    let sender = session.sender();
    let repo = handle.get();
    let mut rx = repo.subscribe();

    let handle = session.runtime().spawn(async move {
        loop {
            rx.recv().await;
            sender.send(port, ());
        }
    });

    UniqueHandle::new(Box::new(handle))
}

/// Cancel the repository change notifications subscription.
#[no_mangle]
pub unsafe extern "C" fn subscription_cancel(handle: UniqueHandle<JoinHandle<()>>) {
    handle.release().abort();
}

pub(super) fn entry_type_to_num(entry_type: JointEntryType) -> u8 {
    match entry_type {
        JointEntryType::File => ENTRY_TYPE_FILE,
        JointEntryType::Directory => ENTRY_TYPE_DIRECTORY,
    }
}
