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
        if self.max_buffer_size == 0 {
            self.writable_file.append(data).await?;
        } else if self.buf.is_empty() && data.len() >= self.max_buffer_size {
            self.writable_file.append(data).await?;
        } else {
            self.buf.extend_from_slice(data);
            if self.buf.len() >= self.max_buffer_size {
                self.writable_file.append(&self.buf).await?;
                self.buf.clear();
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.writable_file.append(&self.buf).await?;
            self.buf.clear();
        }
        Ok(())
    }

    pub async fn pad(&mut self, pad_bytes: usize) -> Result<()> {
        self.file_size += pad_bytes;
        if self.buf.is_empty() {
            self.buf.resize(pad_bytes, 0);
            self.writable_file.append(&self.buf).await?;
        } else if pad_bytes < 100 {
            let pad: [u8; 100] = [0u8; 100];
            self.append(&pad[..pad_bytes]).await?;
        } else {
            let pad = vec![0u8; pad_bytes];
            self.append(&pad).await?;
        }
        Ok(())
    }

    pub async fn sync(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.flush().await?;
        }
        self.writable_file.sync().await?;
        Ok(())
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }
}
