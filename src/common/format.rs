use crate::common::DISABLE_GLOBAL_SEQUENCE_NUMBER;
use crate::util::decode_fixed_uint64;

#[derive(Eq, PartialEq, Clone)]
pub enum CompressionType {
    NoCompression = 0x0,
    SnappyCompression = 0x1,
    ZlibCompression = 0x2,
    BZip2Compression = 0x3,
    LZ4Compression = 0x4,
    LZ4HCCompression = 0x5,
    XpressCompression = 0x6,
    ZSTD = 0x7,

    // Only use ZSTDNotFinalCompression if you have to use ZSTD lib older than
    // 0.8.0 or consider a possibility of downgrading the service or copying
    // the database files to another service running with an older version of
    // RocksDB that doesn't have ZSTD. Otherwise, you should use ZSTD. We will
    // eventually remove the option from the public API.
    ZSTDNotFinalCompression = 0x40,

    // DisableCompressionOption is used to disable some compression options.
    DisableCompressionOption = 0xff,
}

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum ValueType {
    TypeDeletion = 0x0,
    TypeValue = 0x1,
    TypeMerge = 0x2,
    TypeLogData = 0x3,              // WAL only.
    TypeColumnFamilyDeletion = 0x4, // WAL only.
    TypeColumnFamilyValue = 0x5,    // WAL only.
    TypeColumnFamilyMerge = 0x6,    // WAL only.

    TypeColumnFamilyRangeDeletion = 0xE, // WAL only.
    TypeRangeDeletion = 0xF,             // meta block
    TypeColumnFamilyBlobIndex = 0x10,    // Blob DB only
    TypeBlobIndex = 0x11,                // Blob DB only
    MaxValue = 0x7F,                     // Not used for storing records.
}

impl From<u8> for ValueType {
    fn from(x: u8) -> Self {
        unsafe { std::mem::transmute(x) }
    }
}

#[derive(Default, Clone)]
pub struct Slice {
    pub offset: usize,
    pub limit: usize,
}

impl Slice {
    pub fn len(&self) -> usize {
        if self.offset > self.limit {
            0
        } else {
            self.limit - self.offset
        }
    }
}

pub const VALUE_TYPE_FOR_SEEK: u8 = ValueType::TypeBlobIndex as u8;
pub const VALUE_TYPE_FOR_SEEK_FOR_PREV: u8 = ValueType::TypeDeletion as u8;

pub fn pack_sequence_and_type(seq: u64, t: u8) -> u64 {
    (seq << 8) | t as u64
}

pub fn extract_user_key(key: &[u8]) -> &[u8] {
    let l = key.len();
    &key[..(l - 8)]
}

pub fn extract_internal_key_footer(key: &[u8]) -> u64 {
    unsafe { u64::from_le_bytes(*(key as *const _ as *const [u8; 8])) }
}

pub fn extract_value_type(key: &[u8]) -> u8 {
    let l = key.len();
    assert!(l >= 8);
    let num = extract_internal_key_footer(&key[(l - 8)..]);
    (num & 0xffu64) as u8
}

pub fn is_value_type(t: u8) -> bool {
    t <= ValueType::TypeMerge as u8 || t == ValueType::TypeBlobIndex as u8
}

pub fn is_extended_value_type(t: u8) -> bool {
    t <= ValueType::TypeMerge as u8
        || t == ValueType::TypeBlobIndex as u8
        || t == ValueType::TypeRangeDeletion as u8
}

pub struct GlobalSeqnoAppliedKey {
    internal_key: Vec<u8>,
    buf: Vec<u8>,
    global_seqno: u64,
    is_user_key: bool,
}

impl GlobalSeqnoAppliedKey {
    pub fn new(global_seqno: u64, is_user_key: bool) -> Self {
        Self {
            internal_key: vec![],
            buf: vec![],
            global_seqno,
            is_user_key,
        }
    }

    pub fn get_key(&self) -> &[u8] {
        &self.internal_key
    }

    pub fn get_user_key(&self) -> &[u8] {
        if self.is_user_key {
            &self.internal_key
        } else {
            extract_user_key(&self.internal_key)
        }
    }

    pub fn set_user_key(&mut self, key: &[u8]) {
        self.is_user_key = true;
        self.set_key(key);
    }

    pub fn set_key(&mut self, key: &[u8]) {
        if self.global_seqno == DISABLE_GLOBAL_SEQUENCE_NUMBER {
            self.internal_key.clear();
            self.internal_key.extend_from_slice(key);
            return;
        }
        let tail_offset = key.len() - 8;
        let num = decode_fixed_uint64(&key[tail_offset..]);
        self.buf.clear();
        self.buf.extend_from_slice(key);
        self.internal_key.clear();
        self.internal_key.extend_from_slice(&key[..tail_offset]);
        let num = pack_sequence_and_type(self.global_seqno, (num & 0xff) as u8);
        self.internal_key.extend_from_slice(&num.to_le_bytes());
    }

    pub fn update_internal_key(&mut self, seq: u64, tp: ValueType) {
        let newval = (seq << 8) | ((tp as u8) as u64);
        let l = self.internal_key.len() - 8;
        self.internal_key[l..].copy_from_slice(&newval.to_le_bytes());
    }

    pub fn trim_append(&mut self, key: &[u8], shared: usize) {
        if self.global_seqno == DISABLE_GLOBAL_SEQUENCE_NUMBER {
            self.internal_key.resize(shared, 0);
            self.internal_key.extend_from_slice(key);
            return;
        }
        self.buf.resize(shared, 0);
        self.buf.extend_from_slice(key);
        let tail_offset = self.buf.len() - 8;
        let num = decode_fixed_uint64(&self.buf[tail_offset..]);
        let num = pack_sequence_and_type(self.global_seqno, (num & 0xff) as u8);
        if self.internal_key.len() > self.buf.len() {
            let limit = shared + key.len();
            self.internal_key[shared..limit].copy_from_slice(key);
            self.internal_key.resize(limit, 0);
            self.internal_key[tail_offset..limit].copy_from_slice(&num.to_le_bytes());
            assert_eq!(limit, self.buf.len());
        } else {
            self.internal_key.clear();
            self.internal_key
                .extend_from_slice(&self.buf[..tail_offset]);
            self.internal_key.extend_from_slice(&num.to_le_bytes());
        }
    }
}

pub struct ParsedInternalKey<'a> {
    key: &'a [u8],
    pub tp: ValueType,
    pub sequence: u64,
    user_key: Slice,
}

impl<'a> ParsedInternalKey<'a> {
    pub fn new(key: &'a [u8]) -> Self {
        let l = key.len();
        if l < 8 {
            Self {
                key,
                tp: ValueType::MaxValue,
                sequence: 0,
                user_key: Slice::default(),
            }
        } else {
            let offset = l - 8;
            let x = decode_fixed_uint64(&key[offset..]);
            let c = (x & 0xff) as u8;
            let sequence = x >> 8;
            Self {
                key,
                tp: c.into(),
                sequence,
                user_key: Slice {
                    offset: 0,
                    limit: offset,
                },
            }
        }
    }

    pub fn valid(&self) -> bool {
        self.user_key.limit > 0
    }

    pub fn user_key(&self) -> &[u8] {
        &self.key[..self.user_key.limit]
    }
}
