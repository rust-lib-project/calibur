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
    async fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        self.read_exact(offset, data.len(), data).await
    }
    async fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize>;
    fn file_size(&self) -> usize;
    fn use_direct_io(&self) -> bool {
        false
    }
}

#[async_trait]
pub trait WritableFile: Send {
    async fn append(&mut self, data: &[u8]) -> Result<()>;
    fn truncate(&mut self, offset: u64) -> Result<()>;
    fn allocate(&mut self, offset: u64, len: u64) -> Result<()>;
    async fn sync(&mut self) -> Result<()>;
    async fn fsync(&mut self) -> Result<()>;
    fn use_direct_io(&mut self) -> bool {
        false
    }
    fn get_file_size(&self) -> usize {
        0
    }
}

pub trait FileSystem: Send + Sync {
    fn open_writable_file_in(
        &self,
        path: PathBuf,
        file_name: String,
    ) -> Result<Box<WritableFileWriter>> {
        let f = path.join(file_name);
        self.open_writable_file(f)
    }

    fn open_writable_file(&self, file_name: PathBuf) -> Result<Box<WritableFileWriter>>;

    fn open_random_access_file(
        &self,
        path: PathBuf,
        file_name: String,
    ) -> Result<Box<RandomAccessFileReader>>;
    fn file_exist(&self, path: PathBuf, file_name: String) -> Result<bool>;
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

#[async_trait]
impl WritableFile for InMemFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn truncate(&mut self, offset: u64) -> Result<()> {
        self.buf.resize(offset as usize, 0);
        Ok(())
    }

    fn allocate(&mut self, _offset: u64, _len: u64) -> Result<()> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        self.fsync().await
    }

    async fn fsync(&mut self) -> Result<()> {
        let mut fs = self.fs.lock().unwrap();
        fs.files.insert(self.filename.clone(), self.buf.clone());
        Ok(())
    }
}

#[async_trait]
impl RandomAccessFile for InMemFile {
    async fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        if offset >= self.buf.len() {
            Ok(0)
        } else if offset + data.len() > self.buf.len() {
            let rest = self.buf.len() - offset;
            data.copy_from_slice(&self.buf[offset..(offset + rest)]);
            Ok(rest)
        } else {
            data.copy_from_slice(&self.buf[offset..(offset + data.len())]);
            Ok(data.len())
        }
    }

    async fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize> {
        if offset >= self.buf.len() {
            Ok(0)
        } else if offset + n > self.buf.len() {
            let rest = self.buf.len() - offset;
            data.copy_from_slice(&self.buf[offset..(offset + rest)]);
            Ok(rest)
        } else {
            data.copy_from_slice(&self.buf[offset..(offset + n)]);
            Ok(n)
        }
    }
    fn file_size(&self) -> usize {
        self.buf.len()
    }
}

impl FileSystem for InMemFileSystem {
    fn open_writable_file(&self, filename: PathBuf) -> Result<Box<WritableFileWriter>> {
        let f = InMemFile {
            fs: self.inner.clone(),
            buf: vec![],
            filename: filename.to_str().unwrap().to_string(),
        };
        Ok(Box::new(WritableFileWriter::new(
            Box::new(f),
            filename.to_str().unwrap().to_string(),
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

    fn file_exist(&self, _path: PathBuf, filename: String) -> Result<bool> {
        let fs = self.inner.lock().unwrap();
        Ok(fs.files.get(&filename).is_some())
    }
}
