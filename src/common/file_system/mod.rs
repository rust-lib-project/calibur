mod posix_file;
mod reader;
mod writer;

use super::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::common::Error;
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
    fn open_writable_file(
        &self,
        path: PathBuf,
        file_name: String,
    ) -> Result<Box<WritableFileWriter>>;
    fn open_random_access_file(
        &self,
        path: PathBuf,
        file_name: String,
    ) -> Result<Box<RandomAccessFileReader>>;
}
#[derive(Default)]
pub struct InMemFileSystemRep {
    files: HashMap<String, Vec<u8>>,
}

#[derive(Default, Clone)]
pub struct InMemFileSystem {
    inner: Arc<Mutex<InMemFileSystemRep>>,
}

#[derive(Default, Clone)]
pub struct InMemFile {
    pub buf: Vec<u8>,
    fs: Arc<Mutex<InMemFileSystemRep>>,
    filename: String,
}

impl WritableFile for InMemFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn truncate(&mut self, offset: u64) -> Result<()> {
        self.buf.resize(offset as usize, 0);
        Ok(())
    }

    fn allocate(&mut self, offset: u64, len: u64) -> Result<()> {
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.fsync()
    }

    fn fsync(&self) -> Result<()> {
        let mut fs = self.fs.lock().unwrap();
        fs.files.insert(self.filename.clone(), self.buf.clone());
        Ok(())
    }
}

#[async_trait]
impl RandomAccessFile for InMemFile {
    async fn read(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize> {
        if offset >= self.buf.len() {
            Ok(0)
        } else if offset + n > self.buf.len() {
            let rest = self.buf.len() - offset;
            data.copy_from_slice(&self.buf[offset..(offset + rest)]);
            Ok(rest)
        } else {
            data.copy_from_slice(&self.buf);
            Ok(self.buf.len())
        }
    }
}

impl FileSystem for InMemFileSystem {
    fn open_writable_file(&self, _: PathBuf, filename: String) -> Result<Box<WritableFileWriter>> {
        let f = InMemFile {
            fs: self.inner.clone(),
            buf: vec![],
            filename: filename.clone(),
        };
        Ok(Box::new(WritableFileWriter::new(
            Box::new(f),
            filename,
            128,
        )))
    }

    fn open_random_access_file(
        &self,
        _: PathBuf,
        filename: String,
    ) -> Result<Box<RandomAccessFileReader>> {
        let fs = self.inner.lock().unwrap();
        match fs.files.get(&filename) {
            None => {
                return Err(Error::InvalidFilename(format!(
                    "file: {} not exists",
                    filename
                )))
            }
            Some(buf) => {
                let f = InMemFile {
                    fs: self.inner.clone(),
                    buf: buf.clone(),
                    filename: filename.clone(),
                };
                Ok(Box::new(RandomAccessFileReader::new(Box::new(f), filename)))
            }
        }
    }
}
