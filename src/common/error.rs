use std::io;
use std::result;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Box<io::Error>),
    #[error("Empty key")]
    EmptyKey,
    #[error("Too long: {0}")]
    TooLong(String),
    #[error("Invalid checksum")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFile(String),
    #[error("Invalid data: {0}")]
    VarDecode(&'static str),
    #[error("Error when reading table: {0}")]
    TableRead(String),
    #[error("Database Closed")]
    DBClosed,
    #[error("Task Cancel because of: {0}")]
    Cancel(&'static str),
    #[error("Error when reading from log: {0}")]
    LogRead(String),
    #[error("Invalid column family id: {0}")]
    InvalidColumnFamily(u32),
    #[error("Error when compaction: {0}")]
    CompactionError(String),
    #[error("Other Error: {0}")]
    Other(String),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(Box::new(e))
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::Config(e) => Error::Config(e.clone()),
            Error::Io(e) => Error::Other(format!("IO Error: {:?}", e)),
            Error::EmptyKey => Error::EmptyKey,
            Error::TooLong(e) => Error::TooLong(e.clone()),
            Error::InvalidChecksum(e) => Error::InvalidChecksum(e.clone()),
            Error::InvalidFile(e) => Error::InvalidFile(e.clone()),
            Error::VarDecode(e) => Error::VarDecode(*e),
            Error::TableRead(e) => Error::TableRead(e.clone()),
            Error::DBClosed => Error::DBClosed,
            Error::Cancel(e) => Error::Cancel(*e),
            Error::LogRead(e) => Error::LogRead(e.clone()),
            Error::InvalidColumnFamily(e) => Error::InvalidColumnFamily(*e),
            Error::Other(e) => Error::Other(e.clone()),
            Error::CompactionError(e) => Error::CompactionError(e.clone()),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
