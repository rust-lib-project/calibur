use super::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::common::SequentialFileReader;
use crate::common::{format::Slice, Error, Result};
use crate::log::RecordError;

pub struct LogReader {
    reader: Box<SequentialFileReader>,
    buffer: Vec<u8>,
    data: Slice,
    end_of_buffer_offset: usize,
    eof: bool,
    last_record_offset: usize,
}

impl LogReader {
    pub async fn read_record(&mut self, record: &mut Vec<u8>) -> Result<bool> {
        let mut prospective_record_offset = 0;
        let mut in_fragmented_record = false;
        record.clear();
        loop {
            let physical_record_offset = self.end_of_buffer_offset - self.data.len();
            let (fragment, record_type) = self.read_physical_record().await?;
            if record_type < RecordType::RecyclableLastType as u8 {
                let fragment_type = record_type.into();
                match fragment_type {
                    RecordType::ZeroType => {}
                    RecordType::FullType => {
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                        prospective_record_offset = physical_record_offset;
                        self.last_record_offset = prospective_record_offset;
                        return Ok(true);
                    }
                    RecordType::FirstType => {
                        prospective_record_offset = physical_record_offset;
                        in_fragmented_record = true;
                        record.clear();
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                    }
                    RecordType::MiddleType => {
                        if !in_fragmented_record {
                            return Err(Error::LogRead(format!(
                                "missing start of fragmented record({})",
                                fragment.len()
                            )));
                        }
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                    }
                    RecordType::LastType => {
                        if !in_fragmented_record {
                            return Err(Error::LogRead(format!(
                                "missing start of fragmented record({})",
                                fragment.len()
                            )));
                        }
                        self.last_record_offset = prospective_record_offset;
                        record.extend_from_slice(&self.buffer[fragment.offset..fragment.limit]);
                        return Ok(true);
                    }
                    _ => {
                        return Err(Error::LogRead(format!("not support open recycle log")));
                    }
                }
            } else {
                let err_type = record_type.into();
                match err_type {
                    RecordError::Eof => {
                        if in_fragmented_record {
                            record.clear();
                        }
                        return Ok(false);
                    }
                    RecordError::BadRecord
                    | RecordError::BadRecordLen
                    | RecordError::BadRecordChecksum
                    | RecordError::OldRecord => {
                        if in_fragmented_record {
                            record.clear();
                            in_fragmented_record = false;
                        }
                    }
                    _ => {
                        return Ok(false);
                    }
                }
            }
        }
    }

    pub fn last_record_offset(&self) -> usize {
        self.last_record_offset
    }

    async fn read_physical_record(&mut self) -> Result<(Slice, u8)> {
        loop {
            let mut fragment = Slice::default();
            if self.data.len() < HEADER_SIZE {
                let mut r = RecordError::Eof as u8;
                if !self.try_read_more(&mut r).await {
                    return Ok((fragment, r));
                }
                continue;
            }
            let header = &self.buffer[self.data.offset..];
            let a = (header[4] as u32) & 0xff;
            let b = (header[5] as u32) & 0xff;
            let tp = header[6];
            if tp >= RecordType::RecyclableFullType as u8 {
                return Err(Error::LogRead(format!("not support open recycle log")));
            }
            let l = (a | (b << 8)) as usize;
            if l + HEADER_SIZE > self.data.len() {
                self.data.limit = 0;
                self.data.offset = 0;
                if !self.eof {
                    return Err(Error::LogRead(format!("read log header error")));
                } else {
                    return Ok((fragment, RecordError::Eof as u8));
                }
            }
            if tp == RecordType::ZeroType as u8 && l == 0 {
                return Ok((fragment, RecordError::BadRecord as u8));
            }
            // TODO: checksum
            fragment.offset = self.data.offset + HEADER_SIZE;
            fragment.limit = fragment.offset + l;
            self.data.offset += HEADER_SIZE + l;
            return Ok((fragment, tp));
        }
    }

    async fn try_read_more(&mut self, err: &mut u8) -> bool {
        if self.eof {
            *err = RecordError::Eof as u8;
            self.data.limit = 0;
            self.data.offset = 0;
            return false;
        }
        if self.buffer.len() < BLOCK_SIZE {
            self.buffer.resize(BLOCK_SIZE, 0);
        }
        match self.reader.read(&mut self.buffer[..BLOCK_SIZE]).await {
            Ok(r) => {
                self.end_of_buffer_offset += r;
                self.data.offset = 0;
                self.data.limit = r;
                if r < BLOCK_SIZE {
                    self.eof = true;
                }
                true
            }
            Err(_) => {
                *err = RecordError::Eof as u8;
                false
            }
        }
    }
}
