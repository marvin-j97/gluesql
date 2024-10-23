use {
    gluesql_core::error::{AlterTableError, Error, IndexError},
    std::{str, time},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug)]
pub enum TransactionError {
    Abort(&'static str),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Abort(msg) => write!(f, "{msg}"),
        }
    }
}

#[derive(ThisError, Debug)]
pub enum StorageError {
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[error(transparent)]
    Index(#[from] IndexError),

    #[error(transparent)]
    Fjall(#[from] fjall::Error),

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    Str(#[from] str::Utf8Error),

    #[error(transparent)]
    SystemTime(#[from] time::SystemTimeError),

    #[error(transparent)]
    TryFromSlice(#[from] std::array::TryFromSliceError),

    #[error(transparent)]
    TransactionError(#[from] TransactionError),
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Error {
        use StorageError::*;

        match e {
            Fjall(e) => Error::StorageMsg(e.to_string()),
            Bincode(e) => Error::StorageMsg(e.to_string()),
            Str(e) => Error::StorageMsg(e.to_string()),
            SystemTime(e) => Error::StorageMsg(e.to_string()),
            TryFromSlice(e) => Error::StorageMsg(e.to_string()),
            AlterTable(e) => e.into(),
            Index(e) => e.into(),
            TransactionError(e) => Error::StorageMsg(e.to_string()),
        }
    }
}

pub fn err_into<E>(e: E) -> Error
where
    E: Into<StorageError>,
{
    let e: StorageError = e.into();
    let e: Error = e.into();

    e
}
