use camino::Utf8PathBuf;
use dokan::{
    init, shutdown, unmount, CreateFileInfo, DiskSpaceInfo, FileInfo, FileSystemHandler,
    FileSystemMounter, FileTimeOperation, FillDataError, FillDataResult, FindData, MountFlags,
    MountOptions, OperationInfo, OperationResult, VolumeInfo, IO_SECURITY_CONTEXT,
};
use dokan_sys::win32::{
    FILE_CREATE, FILE_DELETE_ON_CLOSE, FILE_DIRECTORY_FILE, FILE_MAXIMUM_DISPOSITION, FILE_OPEN,
    FILE_OPEN_IF, FILE_OVERWRITE, FILE_OVERWRITE_IF, FILE_SUPERSEDE,
};
use ouisync_lib::{deadlock::AsyncMutex, File, JointEntryRef, Repository};
use std::{
    collections::{hash_map, HashMap},
    io::{self, SeekFrom},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    thread,
    time::UNIX_EPOCH,
};
use widestring::{U16CStr, U16CString};
use winapi::{shared::ntstatus::*, um::winnt};

// Use the same value as NTFS.
pub const MAX_COMPONENT_LENGTH: u32 = 255;

pub fn mount(
    runtime_handle: tokio::runtime::Handle,
    repository: Arc<Repository>,
    mount_point: impl AsRef<Path>,
) -> Result<MountGuard, io::Error> {
    // TODO: Check these flags.
    let mut flags = MountFlags::empty();
    //flags |= ALT_STREAM;
    flags |= MountFlags::DEBUG | MountFlags::STDERR;
    flags |= MountFlags::REMOVABLE;

    let options = MountOptions {
        single_thread: false,
        flags,
        ..Default::default()
    };

    let mount_point = match U16CString::from_os_str(mount_point.as_ref().as_os_str()) {
        Ok(mount_point) => mount_point,
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to convert mount point to U16CString: {error:?}"),
            ));
        }
    };

    let (on_mount_tx, on_mount_rx) = oneshot::channel();
    let (unmount_tx, unmount_rx) = mpsc::sync_channel(1);

    let join_handle = thread::spawn(move || {
        let handler = VirtualFilesystem::new(runtime_handle, repository);

        // TODO: Ensure this is done only once.
        init();

        let mut mounter = FileSystemMounter::new(&handler, &mount_point, &options);

        let file_system = match mounter.mount() {
            Ok(file_system) => file_system,
            Err(error) => {
                on_mount_tx
                    .send(Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to mount: {error:?}"),
                    )))
                    .unwrap_or(());
                return;
            }
        };

        // Tell the main thread we've successfully mounted.
        on_mount_tx.send(Ok(())).unwrap_or(());

        // Wait here to preserve `file_system`'s lifetime.
        unmount_rx.recv().unwrap_or(());

        // If we don't do this then dropping `file_system` will block.
        if !unmount(&mount_point) {
            tracing::warn!("Failed to unmount {mount_point:?}");
        }

        drop(file_system);

        shutdown();
    });

    if let Err(error) = on_mount_rx.recv().unwrap() {
        return Err(error);
    }

    Ok(MountGuard {
        unmount_tx,
        join_handle: Some(join_handle),
    })
}

pub struct MountGuard {
    unmount_tx: mpsc::SyncSender<()>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            self.unmount_tx.try_send(()).unwrap_or(());
            join_handle.join().unwrap_or(());
        }
    }
}

type Handles = HashMap<Utf8PathBuf, Arc<AsyncMutex<Shared>>>;

struct Shared {
    id: u64,
    path: Utf8PathBuf,
    handle_count: usize,
    delete_on_close: bool,
}

struct VirtualFilesystem {
    rt: tokio::runtime::Handle,
    repo: Arc<Repository>,
    handles: Arc<AsyncMutex<Handles>>,
    next_id: AtomicU64,
}

impl VirtualFilesystem {
    fn new(rt: tokio::runtime::Handle, repo: Arc<Repository>) -> Self {
        Self {
            rt,
            repo,
            handles: Arc::new(AsyncMutex::new(Default::default())),
            next_id: AtomicU64::new(1),
        }
    }

    fn generate_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn get_or_set_shared(
        &self,
        path: Utf8PathBuf,
        delete_on_close: bool,
    ) -> (Arc<AsyncMutex<Shared>>, u64) {
        match self.handles.lock().await.entry(path.clone()) {
            hash_map::Entry::Occupied(entry) => {
                let shared = entry.get().clone();
                let mut lock = shared.lock().await;
                lock.handle_count += 1;
                lock.delete_on_close |= delete_on_close;
                let id = lock.id;
                drop(lock);
                (shared, id)
            }
            hash_map::Entry::Vacant(entry) => {
                let id = self.generate_id();
                let shared = Arc::new(AsyncMutex::new(Shared {
                    id,
                    path: path.clone(),
                    handle_count: 1,
                    delete_on_close,
                }));
                entry.insert(shared.clone());
                (shared, id)
            }
        }
    }

    async fn create_file_entry(
        &self,
        path: &Utf8PathBuf,
        create_disposition: u32,
    ) -> Result<(File, bool), Error> {
        match create_disposition {
            FILE_CREATE => {
                let mut file = self.repo.create_file(path).await?;
                file.flush().await?;
                Ok((file, true))
            }
            FILE_OPEN => {
                let file = self.repo.open_file(path).await?;
                Ok((file, false))
            }
            FILE_OPEN_IF => match self.repo.open_file(path).await {
                Ok(file) => Ok((file, false)),
                Err(ouisync_lib::Error::EntryNotFound) => {
                    let mut file = self.repo.create_file(path).await?;
                    file.flush().await?;
                    Ok((file, true))
                }
                Err(other) => Err(other.into()),
            },
            // TODO
            FILE_SUPERSEDE | FILE_OVERWRITE | FILE_OVERWRITE_IF => {
                Err(STATUS_NOT_IMPLEMENTED.into())
            }
            _ => Err(STATUS_INVALID_PARAMETER.into()),
        }
    }

    async fn create_directory_entry(
        &self,
        path: &Utf8PathBuf,
        create_disposition: u32,
    ) -> Result<bool, Error> {
        match create_disposition {
            FILE_SUPERSEDE => todo!(),
            FILE_CREATE => {
                let _dir = self.repo.create_directory(&path).await?;
                // TODO: We don't actually know whether it's newly created here.
                Ok(true)
            }
            FILE_OPEN => {
                let _joint_dir = self.repo.open_directory(&path).await?;
                Ok(false)
            }
            FILE_OPEN_IF => todo!(),
            FILE_OVERWRITE => todo!(),
            FILE_OVERWRITE_IF => todo!(),
            _ => Err(STATUS_INVALID_PARAMETER.into()),
        }
    }

    async fn create_entry(
        &self,
        path: Utf8PathBuf,
        is_directory: bool,
        create_disposition: u32,
        delete_on_close: bool,
    ) -> Result<(Entry, bool, u64), Error> {
        let (shared, id) = self.get_or_set_shared(path.clone(), delete_on_close).await;

        let result = if !is_directory {
            self.create_file_entry(&path, create_disposition)
                .await
                .map(|(file, created)| {
                    (
                        Entry::File(FileEntry {
                            file: AsyncMutex::new(Some(file)),
                            shared,
                        }),
                        created,
                    )
                })
        } else {
            self.create_directory_entry(&path, create_disposition)
                .await
                .map(|created| (Entry::Directory(path.clone()), created))
        };

        if result.is_err() {
            match self.handles.lock().await.entry(path) {
                hash_map::Entry::Occupied(entry) => {
                    let mut handle = entry.get().lock().await;
                    handle.handle_count -= 1;
                    if handle.handle_count == 0 {
                        drop(handle);
                        entry.remove();
                    }
                }
                // Unreachable because we ensured it's occupied above.
                hash_map::Entry::Vacant(_) => unreachable!(),
            }
        }

        result.map(|(entry, is_new)| (entry, is_new, id))
    }

    async fn resize_file(&self, file: &mut File, desired_len: u64) -> Result<(), Error> {
        let start_len = file.len();

        if start_len == desired_len {
            return Ok(());
        }

        let local_branch = self.repo.local_branch()?;

        file.fork(local_branch).await?;

        if start_len > desired_len {
            file.truncate(desired_len).await?;
        } else {
            let start_pos = file.seek(SeekFrom::Current(0)).await?;
            file.seek(SeekFrom::End(0)).await?;
            // TODO: We shouldn't need to do allocation here as we're only writing constants.
            let buf = vec![0; (desired_len - start_len) as usize];
            file.write(&buf).await?;
            file.seek(SeekFrom::Start(start_pos)).await?;
        }

        Ok(())
    }

    async fn write_file_impl(
        &self,
        file_entry: &FileEntry,
        offset: Option<u64>,
        data: &[u8],
    ) -> Result<u32, Error> {
        let mut lock = file_entry.file.lock().await;
        let file = lock.as_mut().ok_or(STATUS_FILE_CLOSED)?;

        let offset = match offset {
            Some(offset) => offset,
            None => file.len(),
        };

        let local_branch = self.repo.local_branch()?;

        file.seek(SeekFrom::Start(offset)).await?;
        file.fork(local_branch).await?;
        file.write(data).await?;

        Ok(data.len().try_into().unwrap_or(u32::MAX))
    }

    async fn close_file_impl(&self, entry: &FileEntry) {
        let mut file_lock = entry.file.lock().await;

        let file = match file_lock.as_mut() {
            Some(file) => file,
            None => {
                tracing::error!("File already closed");
                return;
            }
        };

        if let Err(error) = file.flush().await {
            tracing::error!("Failed to flush on file close: {error:?}");
        }

        // Note: `handles` must never be locked *after* `entry.shared`.
        let mut handles = self.handles.lock().await;
        let mut lock = entry.shared.lock().await;

        match handles.entry(lock.path.clone()) {
            hash_map::Entry::Occupied(occupied) => {
                assert_eq!(Arc::as_ptr(occupied.get()), Arc::as_ptr(&entry.shared));

                lock.handle_count -= 1;

                if lock.handle_count == 0 {
                    let to_delete = if lock.delete_on_close {
                        Some(lock.path.clone())
                    } else {
                        None
                    };

                    // Close the file handle.
                    file_lock.take();
                    drop(lock);
                    drop(file_lock);
                    occupied.remove();

                    // Now all file handles to this particular file are closed, so we
                    // shouldn't get the `ouisync_lib::Error::Locked` error.
                    if let Some(to_delete) = to_delete {
                        if let Err(error) = self.repo.remove_entry(to_delete.clone()).await {
                            tracing::warn!(
                                "Failed to delete file \"{to_delete:?}\" on close: {error:?}"
                            );
                        }
                    }
                }
            }
            // This FileEntry exists, so it must be in `handles`.
            hash_map::Entry::Vacant(_) => unreachable!(),
        }
    }

    async fn find_files_impl(
        &self,
        mut fill_find_data: impl FnMut(&FindData) -> FillDataResult,
        directory_path: &Utf8PathBuf,
        pattern: Option<&U16CStr>,
    ) -> Result<(), Error> {
        let dir = self.repo.open_directory(directory_path).await?;

        for entry in dir.entries() {
            let (attributes, file_size) = match &entry {
                JointEntryRef::File(file) => {
                    let file_size = match file.open().await {
                        Ok(file) => file.len(),
                        Err(_) => 0,
                    };
                    (winnt::FILE_ATTRIBUTE_NORMAL, file_size)
                }
                JointEntryRef::Directory(_) => {
                    // TODO: Count block sizes
                    (winnt::FILE_ATTRIBUTE_DIRECTORY, 0)
                }
            };

            // TODO: Unwrap
            let file_name = U16CString::from_str(entry.unique_name().as_ref()).unwrap();

            if let Some(pattern) = pattern {
                let ignore_case = true;
                if !dokan::is_name_in_expression(pattern, &file_name, ignore_case) {
                    continue;
                }
            }

            fill_find_data(&FindData {
                attributes,
                // TODO
                creation_time: UNIX_EPOCH,
                last_access_time: UNIX_EPOCH,
                last_write_time: UNIX_EPOCH,
                file_size,
                file_name,
            })
            .or_else(ignore_name_too_long)?;
        }
        Ok(())
    }
}

//fn create_disposition_to_str(create_disposition: u32) -> &'static str {
//    match create_disposition {
//        // If the file already exists, replace it with the given file. If it does not, create the given file.
//        FILE_SUPERSEDE => "FILE_SUPERSEDE",
//        // If the file already exists, fail the request and do not create or open the given file. If it does not, create the given file.
//        FILE_CREATE => "FILE_CREATE",
//        // If the file already exists, open it instead of creating a new file. If it does not, fail the request and do not create a new file.
//        FILE_OPEN => "FILE_OPEN",
//        // If the file already exists, open it. If it does not, create the given file.
//        FILE_OPEN_IF => "FILE_OPEN_IF",
//        //  If the file already exists, open it and overwrite it. If it does not, fail the request.
//        FILE_OVERWRITE => "FILE_OVERWRITE",
//        //  If the file already exists, open it and overwrite it. If it does not, create the given file.
//        FILE_OVERWRITE_IF => "FILE_OVERWRITE_IF",
//        _ => "UNKNOWN",
//    }
//}

struct FileEntry {
    file: AsyncMutex<Option<File>>,
    shared: Arc<AsyncMutex<Shared>>,
}

enum Entry {
    File(FileEntry),
    Directory(Utf8PathBuf),
}

impl Entry {
    fn as_file(&self) -> Result<&FileEntry, Error> {
        match self {
            Entry::File(file_entry) => Ok(file_entry),
            Entry::Directory(_) => Err(STATUS_INVALID_DEVICE_REQUEST.into()),
        }
    }

    fn as_directory(&self) -> Result<&Utf8PathBuf, Error> {
        match self {
            Entry::File(_) => Err(STATUS_INVALID_DEVICE_REQUEST.into()),
            Entry::Directory(path) => Ok(path),
        }
    }
}

struct EntryHandle {
    id: u64,
    entry: Entry,
}

//  https://dokan-dev.github.io/dokany-doc/html/struct_d_o_k_a_n___o_p_e_r_a_t_i_o_n_s.html
impl<'c, 'h: 'c> FileSystemHandler<'c, 'h> for VirtualFilesystem {
    type Context = EntryHandle;

    // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea
    // https://learn.microsoft.com/en-us/windows/win32/api/winternl/nf-winternl-ntcreatefile
    fn create_file(
        &'h self,
        file_name: &U16CStr,
        _security_context: &IO_SECURITY_CONTEXT,
        _desired_access: winnt::ACCESS_MASK,
        _file_attributes: u32,
        _share_access: u32,
        create_disposition: u32,
        create_options: u32,
        _info: &mut OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<CreateFileInfo<Self::Context>> {
        if create_disposition > FILE_MAXIMUM_DISPOSITION {
            return Err(STATUS_INVALID_PARAMETER);
        }

        let delete_on_close = create_options & FILE_DELETE_ON_CLOSE > 0;
        let is_dir = create_options & FILE_DIRECTORY_FILE > 0;

        let path = to_path(file_name)?;

        let (entry, is_new, id) = self.rt.block_on(async {
            self.create_entry(path, is_dir, create_disposition, delete_on_close)
                .await
        })?;

        Ok(CreateFileInfo {
            context: EntryHandle { id, entry },
            is_dir,
            new_file_created: is_new,
        })
    }

    fn close_file(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) {
        match &context.entry {
            Entry::File(entry) => {
                self.rt.block_on(async {
                    self.close_file_impl(entry).await;
                });
            }
            Entry::Directory(_) => (),
        };
    }

    fn read_file(
        &'h self,
        _file_name: &U16CStr,
        offset: i64,
        buffer: &mut [u8],
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<u32> {
        let entry = match &context.entry {
            Entry::File(entry) => entry,
            Entry::Directory(_) => return Err(STATUS_ACCESS_DENIED),
        };

        let len = self.rt.block_on(async {
            let mut lock = entry.file.lock().await;
            let file = lock.as_mut().ok_or(STATUS_FILE_CLOSED)?;
            let offset: u64 = offset
                .try_into()
                .map_err(|_| ouisync_lib::Error::OffsetOutOfRange)?;
            file.seek(SeekFrom::Start(offset)).await?;
            let size = file.read(buffer).await?;
            Ok::<_, Error>(size)
        })?;

        Ok(len as u32)
    }

    fn write_file(
        &'h self,
        _file_name: &U16CStr,
        offset: i64,
        buffer: &[u8],
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<u32> {
        match &context.entry {
            Entry::File(file) => {
                assert!(offset >= 0);

                let offset: Option<u64> = if info.write_to_eof() {
                    // Will be eof.
                    None
                } else {
                    Some(offset.try_into().map_err(|_| STATUS_INVALID_PARAMETER)?)
                };

                let amount_written = self
                    .rt
                    .block_on(async { self.write_file_impl(file, offset, buffer).await })?;

                Ok(amount_written)
            }
            Entry::Directory(_) => Err(STATUS_ACCESS_DENIED),
        }
    }

    fn flush_file_buffers(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn get_file_information(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<FileInfo> {
        let (attributes, file_size) = match &context.entry {
            Entry::File(entry) => {
                let len = self.rt.block_on(async {
                    let len = entry
                        .file
                        .lock()
                        .await
                        .as_ref()
                        .map(|file| file.len())
                        .ok_or(STATUS_FILE_CLOSED)?;
                    Ok::<_, i32>(len)
                })?;
                (winnt::FILE_ATTRIBUTE_NORMAL, len)
            }
            Entry::Directory(_) => (
                winnt::FILE_ATTRIBUTE_DIRECTORY,
                // TODO: Should we count the blocks?
                0,
            ),
        };

        Ok(FileInfo {
            attributes,
            // TODO
            creation_time: UNIX_EPOCH,
            last_access_time: UNIX_EPOCH,
            last_write_time: UNIX_EPOCH,
            file_size,
            number_of_links: 1,
            file_index: context.id,
        })
    }

    fn find_files(
        &'h self,
        _file_name: &U16CStr,
        fill_find_data: impl FnMut(&FindData) -> FillDataResult,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let dir_path = context.entry.as_directory()?;
        self.rt
            .block_on(async { self.find_files_impl(fill_find_data, dir_path, None).await })
            .map_err(|e| e.into())
    }

    fn find_files_with_pattern(
        &'h self,
        _file_name: &U16CStr,
        pattern: &U16CStr,
        fill_find_data: impl FnMut(&FindData) -> FillDataResult,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let dir_path = context.entry.as_directory()?;
        self.rt
            .block_on(async {
                self.find_files_impl(fill_find_data, dir_path, Some(pattern))
                    .await
            })
            .map_err(|e| e.into())
    }

    fn set_file_attributes(
        &'h self,
        _file_name: &U16CStr,
        _file_attributes: u32,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        todo!()
    }

    fn set_file_time(
        &'h self,
        _file_name: &U16CStr,
        _creation_time: FileTimeOperation,
        _last_access_time: FileTimeOperation,
        _last_write_time: FileTimeOperation,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        tracing::warn!("set_file_time not implemented yet");
        Ok(())
    }

    fn delete_file(
        &'h self,
        _file_name: &U16CStr,
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        self.rt.block_on(async {
            let file_entry = context.entry.as_file()?;
            file_entry.shared.lock().await.delete_on_close = info.delete_on_close();
            Ok(())
        })
    }

    fn delete_directory(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        todo!()
    }

    fn move_file(
        &'h self,
        _file_name: &U16CStr,
        _new_file_name: &U16CStr,
        _replace_if_existing: bool,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        // Note: Don't forget to rename in `self.handles`.
        todo!()
    }

    fn set_end_of_file(
        &'h self,
        file_name: &U16CStr,
        offset: i64,
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        // TODO: How do the fwo functions differ?
        self.set_allocation_size(file_name, offset, info, context)
    }

    fn set_allocation_size(
        &'h self,
        _file_name: &U16CStr,
        alloc_size: i64,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let alloc_size: u64 = alloc_size
            .try_into()
            .map_err(|_| STATUS_INVALID_PARAMETER)?;

        let entry = context.entry.as_file()?;

        self.rt.block_on(async {
            let mut lock = entry.file.lock().await;
            let file = lock.as_mut().ok_or(STATUS_FILE_CLOSED)?;
            self.resize_file(file, alloc_size).await?;
            Ok(())
        })
    }

    fn get_disk_free_space(
        &'h self,
        _info: &OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<DiskSpaceInfo> {
        // TODO
        Ok(DiskSpaceInfo {
            byte_count: 1024 * 1024 * 1024,
            free_byte_count: 512 * 1024 * 1024,
            available_byte_count: 512 * 1024 * 1024,
        })
    }

    fn get_volume_information(
        &'h self,
        _info: &OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<VolumeInfo> {
        Ok(VolumeInfo {
            name: U16CString::from_str("ouisync").unwrap(),
            serial_number: 0,
            max_component_length: MAX_COMPONENT_LENGTH,
            fs_flags: winnt::FILE_CASE_PRESERVED_NAMES
                | winnt::FILE_CASE_SENSITIVE_SEARCH
                | winnt::FILE_UNICODE_ON_DISK
                | winnt::FILE_PERSISTENT_ACLS
                | winnt::FILE_NAMED_STREAMS,
            // Custom names don't play well with UAC.
            fs_name: U16CString::from_str("NTFS").unwrap(),
        })
    }

    fn mounted(
        &'h self,
        _mount_point: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn unmounted(&'h self, _info: &OperationInfo<'c, 'h, Self>) -> OperationResult<()> {
        Ok(())
    }
}

fn ignore_name_too_long(err: FillDataError) -> OperationResult<()> {
    match err {
        // Normal behavior.
        FillDataError::BufferFull => Err(STATUS_BUFFER_OVERFLOW),
        // Silently ignore this error because 1) file names passed to create_file should have been checked
        // by Windows. 2) We don't want an error on a single file to make the whole directory unreadable.
        FillDataError::NameTooLong => Ok(()),
    }
}

#[derive(Debug)]
enum Error {
    NtStatus(i32),
    OuiSync(ouisync_lib::Error),
}

impl From<i32> for Error {
    fn from(ntstatus: i32) -> Self {
        Self::NtStatus(ntstatus)
    }
}

impl From<ouisync_lib::Error> for Error {
    fn from(error: ouisync_lib::Error) -> Self {
        Self::OuiSync(error)
    }
}

impl From<Error> for i32 {
    fn from(error: Error) -> i32 {
        match error {
            // List of NTSTATUS values:
            // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-erref/596a1078-e883-4972-9bbc-49e60bebca55
            Error::NtStatus(ntstatus) => ntstatus,
            Error::OuiSync(error) => {
                use ouisync_lib::Error as E;

                match error {
                    E::EntryNotFound => STATUS_OBJECT_NAME_NOT_FOUND,
                    E::PermissionDenied => STATUS_ACCESS_DENIED,
                    E::OffsetOutOfRange => STATUS_INVALID_PARAMETER,
                    E::OperationNotSupported => STATUS_NOT_IMPLEMENTED,
                    E::Locked => STATUS_LOCK_NOT_GRANTED,
                    E::EntryIsDirectory => STATUS_INVALID_DEVICE_REQUEST,
                    _ => todo!("Unhandled error to NTSTATUS conversion \"{error:?}\""),
                }
            }
        }
    }
}

fn to_path(path_cstr: &U16CStr) -> OperationResult<Utf8PathBuf> {
    let path_str: String = match path_cstr.to_string() {
        Ok(path_str) => path_str,
        Err(_) => {
            tracing::warn!("Failed to convert U16CStr to Utf8Path: {path_cstr:?}");
            return Err(STATUS_OBJECT_NAME_INVALID);
        }
    };

    Ok(Utf8PathBuf::from(path_str))
}
