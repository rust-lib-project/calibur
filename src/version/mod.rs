mod column_family;
pub mod manifest;
pub mod snapshot;
mod version;
mod version_set;
pub mod version_storage_info;
use std::sync::Arc;
pub use column_family::ColumnFamily;
pub use version::*;
pub use version_set::VersionSet;
use crate::common::RandomAccessFile;

pub struct FileMetaData {
    level: u32,
    raw_file: Arc<dyn RandomAccessFile>,
    smallest: Vec<u8>,
    largest: Vec<u8>,
    being_compact: bool,
}

pub struct VersionEdit {
    pub add_files: Vec<FileMetaData>,
    pub deleted_files: Vec<FileMetaData>,
}