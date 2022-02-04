use crate::common::Result;
use crate::common::WritableFile;

pub struct WritableFileWriter {
    file_name: String,
    writable_file: Box<dyn WritableFile>,
    buf: Vec<u8>,
    file_size: usize,
    last_sync_size: u64,
    max_buffer_size: usize,
}

impl WritableFileWriter {
    pub fn new(
        writable_file: Box<dyn WritableFile>,
        file_name: String,
        max_buffer_size: usize,
    ) -> Self {
        let file_size = writable_file.get_file_size();
        WritableFileWriter {
            file_name,
            writable_file,
            buf: Vec::with_capacity(std::cmp::min(65536, max_buffer_size)),
            last_sync_size: 0,
            file_size,
            max_buffer_size,
        }
    }

    pub async fn append(&mut self, data: &[u8]) -> Result<()> {
        // TODO: We will cache data in buf when we use direct_io for write operation.
        self.file_size += data.len();
        self.writable_file.append(data).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn pad(&mut self, pad_bytes: usize) -> Result<()> {
        self.file_size += pad_bytes;
        self.buf.resize(pad_bytes, 0);
        self.writable_file.append(&self.buf).await?;
        Ok(())
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.writable_file.sync().await?;
        Ok(())
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }
}
