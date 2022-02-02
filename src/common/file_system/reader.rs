use crate::common::RandomAccessFile;
use crate::common::Result;

pub struct RandomAccessFileReader {
    file: Box<dyn RandomAccessFile>,
    filename: String,
}

impl RandomAccessFileReader {
    pub fn new(file: Box<dyn RandomAccessFile>, filename: String) -> Self {
        Self { file, filename }
    }
    pub async fn read_exact(&self, offset: usize, n: usize, buf: &mut [u8]) -> Result<usize> {
        self.file.read_exact(offset, n, buf).await
    }

    pub async fn read(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.file.read(offset, buf).await
    }

    pub fn name(&self) -> &str {
        self.filename.as_str()
    }

    pub fn use_direct_io(&self) -> bool {
        self.file.use_direct_io()
    }

    pub fn file_size(&self) -> usize {
        self.file.file_size()
    }
}
