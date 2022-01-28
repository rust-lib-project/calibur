mod aio_file;
mod posix_file;
mod reader;
mod writer;

use super::Result;
use std::path::PathBuf;

use async_trait::async_trait;
pub use posix_file::PosixWritableFile;
pub use reader::RandomAccessFileReader;
pub use writer::WritableFileWriter;

#[async_trait]
pub trait RandomAccessFile: 'static + Send + Sync {
    async fn read(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize>;
    fn use_direct_io(&self) -> bool {
        false
    }
}

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
    fn open_random_access_file(
        &self,
        path: PathBuf,
        file_name: String,
    ) -> Result<Box<dyn WritableFile>>;
}
