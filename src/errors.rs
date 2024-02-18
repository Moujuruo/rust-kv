use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedToReadFromDataFile,
    #[error("failed to write to data file")]
    FailedToWriteToDataFile,
    #[error("failed to sync data file")]
    FailedToSyncDataFile,
    #[error("failed to open data file")]
    FailedToOpenDataFile,
    #[error("key is empty")]
    KeyIsEmpty,
}

pub type Result<T> = result::Result<T, Errors>;
