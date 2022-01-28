use crate::common::RandomAccessFile;
use crate::common::Result;

pub struct RandomAccessFileReader {
    file: Box<dyn RandomAccessFile>,
    filename: String,
}

impl RandomAccessFileReader {
    pub async fn read(&self, offset: usize, n: usize, buf: &mut [u8]) -> Result<usize> {
        self.file.read(offset, n, buf).await
    }

    pub async fn read_vec(&self, offset: usize, n: usize, buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    pub fn name(&self) -> &str {
        self.filename.as_str()
    }

    pub fn use_direct_io(&self) -> bool {
        self.file.use_direct_io()
    }
}
