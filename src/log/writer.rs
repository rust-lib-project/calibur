use super::{RecordType, BLOCK_SIZE, HEADER_SIZE, LOG_PADDING, RECYCLABLE_HEADER_SIZE};
use crate::common::{Result, WritableFileWriter};
use crate::util;
use crc32c::crc32c;
use crc32c::crc32c_append;

pub struct LogWriter {
    writer: Box<WritableFileWriter>,
    block_offset: usize,
    log_number: u64,
    type_crc: Vec<u32>,
}

impl LogWriter {
    pub fn new(writer: Box<WritableFileWriter>, log_number: u64) -> Self {
        let type_crc = vec![
            crc32c(&[0]),
            crc32c(&[1]),
            crc32c(&[2]),
            crc32c(&[3]),
            crc32c(&[4]),
        ];
        LogWriter {
            writer,
            log_number,
            block_offset: 0,
            type_crc,
        }
    }

    pub fn get_file_mut(&mut self) -> &mut WritableFileWriter {
        self.writer.as_mut()
    }

    pub fn get_file_size(&self) -> usize {
        self.writer.file_size()
    }

    pub async fn fsync(&mut self) -> Result<()> {
        self.writer.sync().await
    }

    pub async fn add_record(&mut self, data: &[u8]) -> Result<()> {
        let mut left = data.len();
        let mut begin = true;
        let mut offset = 0;
        while left > 0 {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    self.writer.append(&LOG_PADDING[..leftover]).await?;
                }
                self.block_offset = 0;
            }
            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = std::cmp::min(left, avail);
            let record_type = if begin && left == fragment_length {
                RecordType::FullType
            } else if begin {
                RecordType::FirstType
            } else if left == fragment_length {
                RecordType::LastType
            } else {
                RecordType::MiddleType
            };
            self.emit_physical_record(record_type, &data[offset..(offset + fragment_length)])
                .await?;
            offset += fragment_length;
            left -= fragment_length;
            begin = false;
        }
        self.writer.flush().await?;
        Ok(())
    }

    async fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        let mut buf: [u8; RECYCLABLE_HEADER_SIZE] = [0u8; RECYCLABLE_HEADER_SIZE];
        // TODO: We do not support recycle wal log.
        buf[4] = (data.len() & 0xff) as u8;
        buf[5] = (data.len() >> 8) as u8;
        buf[6] = record_type as u8;
        let mut crc = self.type_crc[buf[6] as usize];
        crc = crc32c_append(crc, data);
        crc = util::crc_mask(crc);
        buf[..4].copy_from_slice(&crc.to_le_bytes());
        self.writer.append(&buf[..HEADER_SIZE]).await?;
        self.writer.append(data).await?;
        self.block_offset += HEADER_SIZE + data.len();
        Ok(())
    }
}
