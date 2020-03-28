use thiserror::Error as ThisError;

use crate::executor::{BlendError, FilterError, SelectError};
use crate::ExecuteError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    // storage
    #[error("not found")]
    NotFound,

    #[error(transparent)]
    Execute(#[from] ExecuteError),
    #[error(transparent)]
    Select(#[from] SelectError),
    #[error(transparent)]
    Blend(#[from] BlendError),
    #[error(transparent)]
    Filter(#[from] FilterError),

    // all other errors
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::err!($($arg)*));
    };
}

#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {
        Error::Other(anyhow::anyhow!($($arg)*))
    };
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !$cond {
            $crate::bail!($($arg)*);
        }
    };
}
