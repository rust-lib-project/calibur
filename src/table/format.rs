use crate::common::{Error, Result};
use crate::table::format::ChecksumType::CRC32c;
use crate::util::{decode_fixed_uint32, encode_var_uint64, get_var_uint32, get_var_uint64};

pub const MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX: usize = 1usize << 16;

#[derive(Default, Debug, Clone, Copy)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn encode_to(&self, data: &mut Vec<u8>) {
        let mut tmp: [u8; 20] = [0u8; 20];
        let offset = encode_var_uint64(&mut tmp, self.offset);
        let offset = encode_var_uint64(&mut tmp[offset..], self.size);
        data.extend_from_slice(&tmp[..offset]);
    }

    pub fn decode_from(&mut self, data: &[u8]) -> Result<usize> {
        let offset = match get_var_uint64(data) {
            None => return Err(Error::VarDecode("BlockHandle")),
            Some((val_len, val)) => {
                self.offset = val;
                val_len
            }
        };
        match get_var_uint64(&data[offset..]) {
            None => Err(Error::VarDecode("BlockHandle")),
            Some((val_len, val)) => {
                self.size = val;
                Ok(offset + val_len)
            }
        }
    }
}

pub const NULL_BLOCK_HANDLE: BlockHandle = BlockHandle { offset: 0, size: 0 };

#[derive(Default, Debug, Clone)]
pub struct IndexValue {
    pub handle: BlockHandle,
}

impl IndexValue {
    pub fn decode_from(&mut self, data: &[u8]) -> Result<()> {
        self.handle.decode_from(data)?;
        Ok(())
    }
}

pub struct IndexValueRef {
    pub handle: BlockHandle,
}

impl IndexValueRef {
    pub fn new(handle: BlockHandle) -> Self {
        Self {
            handle,
        }
    }

    pub fn to_owned(&self) -> IndexValue {
        IndexValue {
            handle: self.handle.clone(),
        }
    }

    pub fn encode_to(&self, buff: &mut Vec<u8>) {
        self.handle.encode_to(buff);
        // TODO: support encode the first key
    }
}

impl IndexValue {
    pub fn as_ref(&self) -> IndexValueRef {
        IndexValueRef {
            handle: self.handle.clone(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum ChecksumType {
    NoChecksum = 0x0,
    CRC32c = 0x1,
    xxHash = 0x2,
    xxHash64 = 0x3,
}

pub const LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57u64;
pub const LEGACY_PLAIN_TABLE_MAGIC_NUMBER: u64 = 0x4f3418eb7a8f13b8u64;
pub const BLOCK_BASED_TABLE_MAGIC_NUMBER: u64 = 0x88e241b785f4cff7u64;
pub const PLAIN_TABLE_MAGIC_NUMBER: u64 = 0x8242229663bf9564u64;
const MAGIC_NUMBER_LENGTH_BYTE: usize = 8;

pub fn is_legacy_footer_format(magic_number: u64) -> bool {
    magic_number == LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER
        || magic_number == LEGACY_PLAIN_TABLE_MAGIC_NUMBER
}

#[derive(Default, Clone)]
pub struct Footer {
    pub version: u32,
    pub checksum: u8,
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub table_magic_number: u64,
}

pub const BLOCK_HANDLE_MAX_ENCODED_LENGTH: usize = 20;
pub const VERSION0ENCODED_LENGTH: usize = 2 * BLOCK_HANDLE_MAX_ENCODED_LENGTH + 8;
pub const NEW_VERSIONS_ENCODED_LENGTH: usize = 1 + 2 * BLOCK_HANDLE_MAX_ENCODED_LENGTH + 4 + 8;

impl Footer {
    pub fn set_checksum(&mut self, ck: ChecksumType) {
        self.checksum = ck as u8;
    }

    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        if is_legacy_footer_format(self.table_magic_number) {
            let origin_size = buf.len();
            assert_eq!(self.checksum, ChecksumType::CRC32c as u8);
            self.metaindex_handle.encode_to(buf);
            self.index_handle.encode_to(buf);
            buf.resize(origin_size + BLOCK_HANDLE_MAX_ENCODED_LENGTH * 2, 0);
            let v1 = (self.table_magic_number & 0xffffffffu64) as u32;
            let v2 = (self.table_magic_number >> 32) as u32;
            buf.extend_from_slice(&v1.to_le_bytes());
            buf.extend_from_slice(&v2.to_le_bytes());
            assert_eq!(buf.len(), origin_size + VERSION0ENCODED_LENGTH);
        } else {
            let origin_size = buf.len();
            buf.push(self.checksum);
            self.metaindex_handle.encode_to(buf);
            self.index_handle.encode_to(buf);
            buf.resize(origin_size + NEW_VERSIONS_ENCODED_LENGTH - 12, 0);
            buf.extend_from_slice(&self.version.to_le_bytes());
            let v1 = (self.table_magic_number & 0xffffffffu64) as u32;
            let v2 = (self.table_magic_number >> 32) as u32;
            buf.extend_from_slice(&v1.to_le_bytes());
            buf.extend_from_slice(&v2.to_le_bytes());
            assert_eq!(buf.len(), origin_size + NEW_VERSIONS_ENCODED_LENGTH);
        }
    }

    pub fn decode_from(&mut self, data: &[u8]) -> Result<()> {
        let magic_offset = data.len() - MAGIC_NUMBER_LENGTH_BYTE;
        let magic_lo = decode_fixed_uint32(&data[magic_offset..]);
        let magic_hi = decode_fixed_uint32(&data[(magic_offset + 4)..]);
        let mut magic = ((magic_hi as u64) << 32) | (magic_lo as u64);
        let legacy = is_legacy_footer_format(magic);
        if legacy {
            if magic == LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER {
                magic = BLOCK_BASED_TABLE_MAGIC_NUMBER;
            } else if magic == LEGACY_PLAIN_TABLE_MAGIC_NUMBER {
                magic = PLAIN_TABLE_MAGIC_NUMBER;
            }
        }

        self.table_magic_number = magic;
        let mut offset = if legacy {
            self.version = 0;
            self.checksum = CRC32c as u8;
            data.len() - VERSION0ENCODED_LENGTH
        } else {
            self.version = decode_fixed_uint32(&data[(magic_offset - 4)..]);
            let mut offset = data.len() - NEW_VERSIONS_ENCODED_LENGTH;
            match get_var_uint32(&data[offset..]) {
                None => return Err(Error::VarDecode("BlockBasedTable Footer")),
                Some((val_len, val)) => {
                    self.checksum = val as u8;
                    offset += val_len;
                }
            }
            offset
        };
        offset += self.metaindex_handle.decode_from(&data[offset..])?;
        self.index_handle.decode_from(&data[offset..])?;
        Ok(())
    }
}
