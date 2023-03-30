use crate::config::ConfigError;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;

/// A specialized `Result` type for convenience.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Library(#[from] ouisync_lib::Error),
    #[error("failed to initialize logger")]
    InitializeLogger(#[source] io::Error),
    #[error("failed to initialize runtime")]
    InitializeRuntime(#[source] io::Error),
    #[error("request is malformed")]
    MalformedRequest(#[source] rmp_serde::decode::Error),
    #[error("request failed: {message}")]
    RequestFailed { code: ErrorCode, message: String },
    #[error("request is forbidden")]
    ForbiddenRequest,
    #[error("argument is not valid")]
    InvalidArgument,
    #[error("connection lost")]
    ConnectionLost,
    #[error("failed to read from or write into the config file")]
    Config(#[from] ConfigError),
    #[error("input/output error")]
    Io(#[from] io::Error),
}

impl Error {
    pub fn to_error_code(&self) -> ErrorCode {
        match self {
            Self::Library(error) => {
                use ouisync_lib::Error::*;

                match error {
                    Db(_) => ErrorCode::Db,
                    PermissionDenied => ErrorCode::PermissionDenied,
                    MalformedData | MalformedDirectory => ErrorCode::MalformedData,
                    EntryExists => ErrorCode::EntryExists,
                    EntryNotFound => ErrorCode::EntryNotFound,
                    AmbiguousEntry => ErrorCode::AmbiguousEntry,
                    DirectoryNotEmpty => ErrorCode::DirectoryNotEmpty,
                    OperationNotSupported | ConcurrentWriteNotSupported => {
                        ErrorCode::OperationNotSupported
                    }
                    NonUtf8FileName | OffsetOutOfRange => ErrorCode::InvalidArgument,
                    StorageVersionMismatch => ErrorCode::StorageVersionMismatch,
                    BlockNotFound(_) | BlockNotReferenced | WrongBlockLength(_) | EntryIsFile
                    | EntryIsDirectory | Writer(_) | RequestTimeout => ErrorCode::Other,
                }
            }
            Self::InitializeLogger(_) | Self::InitializeRuntime(_) | Self::Io(_) => {
                ErrorCode::Other
            }
            Self::Config(_) => ErrorCode::Config,
            Self::MalformedRequest(_) => ErrorCode::MalformedRequest,
            Self::RequestFailed { code, .. } => *code,
            Self::InvalidArgument => ErrorCode::InvalidArgument,
            Self::ConnectionLost => ErrorCode::ConnectionLost,
            Self::ForbiddenRequest => ErrorCode::ForbiddenRequest,
        }
    }
}

#[derive(
    Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug, IntoPrimitive, TryFromPrimitive,
)]
#[repr(u16)]
#[serde(into = "u16", try_from = "u16")]
pub enum ErrorCode {
    /// No error
    Ok = 0,
    /// Database error
    Db = 1,
    /// Insuficient permission to perform the intended operation
    PermissionDenied = 2,
    /// Malformed data
    MalformedData = 3,
    /// Entry already exists
    EntryExists = 4,
    /// Entry doesn't exist
    EntryNotFound = 5,
    /// Multiple matching entries found
    AmbiguousEntry = 6,
    /// The intended operation requires the directory to be empty but it isn't
    DirectoryNotEmpty = 7,
    /// The indended operation is not supported
    OperationNotSupported = 8,
    /// Failed to read from or write into the config file
    Config = 10,
    /// Argument passed to a function is not valid
    InvalidArgument = 11,
    /// Interface request is malformed
    MalformedRequest = 12,
    /// Storage format version mismatch
    StorageVersionMismatch = 13,
    /// Connection lost
    ConnectionLost = 14,
    /// Request is forbidden
    ForbiddenRequest = 15,
    /// Unspecified error
    Other = 65535,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_serialize_deserialize() {
        let origs = [
            ErrorCode::Ok,
            ErrorCode::Db,
            ErrorCode::PermissionDenied,
            ErrorCode::MalformedData,
            ErrorCode::EntryExists,
            ErrorCode::EntryNotFound,
            ErrorCode::AmbiguousEntry,
            ErrorCode::DirectoryNotEmpty,
            ErrorCode::OperationNotSupported,
            ErrorCode::Config,
            ErrorCode::InvalidArgument,
            ErrorCode::MalformedRequest,
            ErrorCode::StorageVersionMismatch,
            ErrorCode::ConnectionLost,
            ErrorCode::ForbiddenRequest,
            ErrorCode::Other,
        ];

        for orig in origs {
            let encoded = rmp_serde::to_vec(&orig).unwrap();
            let decoded: ErrorCode = rmp_serde::from_slice(&encoded).unwrap();
            assert_eq!(decoded, orig);
        }
    }
}