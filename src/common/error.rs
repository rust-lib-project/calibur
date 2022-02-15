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
    Cancel(String),
    #[error("Error when reading from log: {0}")]
    LogRead(String),
    #[error("Invalid Log Offset: {0} > {1}")]
    InvalidLogOffset(u32, u32),
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
            Error::TooLong(s) => Error::TooLong(s.clone()),
            Error::InvalidChecksum(s) => Error::InvalidChecksum(s.clone()),
            Error::InvalidFile(s) => Error::InvalidFile(s.clone()),
            Error::VarDecode(x) => Error::VarDecode(*x),
            Error::TableRead(x) => Error::TableRead(x.clone()),
            Error::DBClosed => Error::DBClosed,
            Error::Cancel(e) => Error::Cancel(e.clone()),
            Error::LogRead(x) => Error::LogRead(x.clone()),
            Error::InvalidLogOffset(x, y) => Error::InvalidLogOffset(*x, *y),
            Error::Other(x) => Error::Other(x.clone()),
            Error::CompactionError(s) => Error::CompactionError(s.clone()),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
