mod file;
mod writer;

use super::Result;
use std::path::PathBuf;

pub use file::PosixWritableFile;
pub use writer::WritableFileWriter;

pub trait RandomAccessFileReader: 'static + Send + Sync {}

pub trait WritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()>;
    fn truncate(&mut self, offset: u64) -> Result<()>;
    fn allocate(&mut self, offset: u64, len: u64) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn fsync(&self) -> Result<()>;
    fn use_direct_io(&mut self) -> bool {
        false
    }
    fn get_file_size(&self) -> usize {
        0
    }
}

pub trait FileSystem {
    fn open_writable_file(&self, path: PathBuf, file_name: String)
        -> Result<Box<dyn WritableFile>>;
}
