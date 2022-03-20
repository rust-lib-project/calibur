use crate::common::file_system::SequentialFile;
use crate::common::RandomAccessFile;
use crate::common::Result;

pub struct RandomAccessFileReader {
    file: Box<dyn RandomAccessFile>,
}

impl RandomAccessFileReader {
    pub fn new(file: Box<dyn RandomAccessFile>) -> Self {
        Self { file }
    }

    pub async fn read_exact(&self, offset: usize, n: usize, buf: &mut [u8]) -> Result<usize> {
        self.file.read_exact(offset, n, buf).await
    }

    pub async fn read(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.file.read(offset, buf).await
    }

    pub fn use_direct_io(&self) -> bool {
        self.file.use_direct_io()
    }

    pub fn file_size(&self) -> usize {
        self.file.file_size()
    }
}

pub struct SequentialFileReader {
    file: Box<dyn SequentialFile>,
    filename: String,
}

impl SequentialFileReader {
    pub fn new(file: Box<dyn SequentialFile>, filename: String) -> Self {
        Self { file, filename }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.file.read_sequential(buf).await
    }

    pub fn name(&self) -> &str {
        self.filename.as_str()
    }

    pub fn use_direct_io(&self) -> bool {
        false
    }

    pub fn file_size(&self) -> usize {
        self.file.get_file_size()
    }
}

pub struct BufferedFileReader {
    file: Box<dyn RandomAccessFile>,
    buff: Vec<u8>,
    buff_offset: usize,
}

impl BufferedFileReader {
    pub fn new(file: Box<dyn RandomAccessFile>) -> Self {
        Self {
            file,
            buff: vec![],
            buff_offset: 0,
        }
    }

    pub async fn prefetch(&mut self, offset: usize, length: usize) -> Result<()> {
        self.buff.resize(length, 0);
        let ret = self.file.read(offset, self.buff.as_mut_slice()).await?;
        self.buff.resize(ret, 0);
        Ok(())
    }

    pub async fn read(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        if self.buff.is_empty() {
            return self.file.read(offset, buf).await;
        }
        let mut total_read_size = 0;
        if offset < self.buff_offset {
            let read_size = std::cmp::min(self.buff_offset - offset, buf.len());
            let ret = self.file.read(offset, &mut buf[..read_size]).await?;
            if ret < read_size {
                return Ok(ret);
            }
            total_read_size += ret;
        }

        if self.buff_offset + self.buff.len() > offset && self.buff_offset < offset + buf.len() {
            let copy_left = std::cmp::max(self.buff_offset, offset);
            let copy_right = std::cmp::min(self.buff_offset + self.buff.len(), offset + buf.len());
            buf[(copy_left - offset)..(copy_right - offset)].copy_from_slice(
                &self.buff[(copy_left - self.buff_offset)..(copy_right - self.buff_offset)],
            );
            total_read_size += copy_right - copy_left;
        }
        if self.buff_offset + self.buff.len() < offset + buf.len() {
            let read_offset = std::cmp::max(self.buff_offset + self.buff.len(), offset);
            let ret = self
                .file
                .read(read_offset, &mut buf[(read_offset - offset)..])
                .await?;
            total_read_size += ret;
        }
        Ok(total_read_size)
    }

    pub fn use_direct_io(&self) -> bool {
        self.file.use_direct_io()
    }

    pub fn file_size(&self) -> usize {
        self.file.file_size()
    }

    pub fn release(self) -> Box<dyn RandomAccessFile> {
        self.file
    }
}
