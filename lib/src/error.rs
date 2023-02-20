use crate::{
    block::{BlockId, BLOCK_SIZE},
    db,
};
#[cfg(test)]
use std::backtrace::Backtrace;
use std::{array::TryFromSliceError, fmt, io};

/// A specialized `Result` type for convenience.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// I think `thiserror` is trying to do something clever when the error enum contains a field of
/// type `Backtrace`, but it fails with a strange error if it's so. Wrapping `Backtrace` into a
/// simple wrapper such as this one seems to help.
#[cfg(test)]
pub struct BacktraceWrapper(Backtrace);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database error")]
    Db {
        #[source]
        source: db::Error,
        #[cfg(test)]
        backtrace: BacktraceWrapper,
    },
    #[error("failed to read from or write into the device ID config file")]
    DeviceIdConfig(#[source] io::Error),
    #[error("permission denied")]
    PermissionDenied,
    #[error("data is malformed")]
    MalformedData,
    #[error("block not found: {0}")]
    BlockNotFound(BlockId),
    #[error("block is not referenced by the index")]
    BlockNotReferenced,
    #[error("block has wrong length (expected: {}, actual: {0})", BLOCK_SIZE)]
    WrongBlockLength(usize),
    #[error("not a directory or directory malformed")]
    MalformedDirectory,
    #[error("entry already exists")]
    EntryExists,
    #[error("entry not found")]
    EntryNotFound,
    #[error("entry has multiple concurrent versions")]
    AmbiguousEntry,
    #[error("entry is a file")]
    EntryIsFile,
    #[error("entry is a directory")]
    EntryIsDirectory,
    #[error("File name is not a valid UTF-8 string")]
    NonUtf8FileName,
    #[error("offset is out of range")]
    OffsetOutOfRange,
    #[error("directory is not empty")]
    DirectoryNotEmpty,
    #[error("operation is not supported")]
    OperationNotSupported,
    #[error("failed to initialize logger")]
    InitializeLogger(#[source] io::Error),
    #[error("failed to initialize runtime")]
    InitializeRuntime(#[source] io::Error),
    #[error("failed to write into writer")]
    Writer(#[source] io::Error),
    #[error("request timeout")]
    RequestTimeout,
    #[error("storage version mismatch")]
    StorageVersionMismatch,
    // TODO: remove this error variant when we implement proper write concurrency
    #[error("concurrent write is not supported")]
    ConcurrentWriteNotSupported,
}

impl Error {
    /// Returns an object that implements `Display` which prints this error together with its whole
    /// causal chain.
    pub fn verbose(&self) -> Verbose {
        Verbose(self)
    }
}

#[cfg(test)]
impl fmt::Debug for BacktraceWrapper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self.0)
    }
}

impl From<TryFromSliceError> for Error {
    fn from(_: TryFromSliceError) -> Self {
        Self::MalformedData
    }
}

impl From<db::Error> for Error {
    fn from(src: db::Error) -> Self {
        Self::Db {
            source: src.into(),
            #[cfg(test)]
            backtrace: BacktraceWrapper(Backtrace::capture()),
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(src: sqlx::Error) -> Self {
        Self::Db {
            source: src.into(),
            #[cfg(test)]
            backtrace: BacktraceWrapper(Backtrace::capture()),
        }
    }
}

pub struct Verbose<'a>(&'a Error);

impl fmt::Display for Verbose<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::error::Error;

        writeln!(f, "{}", self.0)?;

        let mut current = self.0 as &dyn Error;

        while let Some(source) = current.source() {
            writeln!(f, "    caused by: {}", source)?;
            current = source;
        }

        Ok(())
    }
}
