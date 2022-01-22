pub const kTypeDeletion: u8 = 0x0;
pub const kTypeValue: u8 = 0x1;
pub const kTypeMerge: u8 = 0x2;
pub const kTypeColumnFamilyDeletion: u8 = 0x4; // WAL only.
pub const kTypeColumnFamilyValue: u8 = 0x5; // WAL only.
pub const kTypeColumnFamilyMerge: u8 = 0x6; // WAL only.
pub const kTypeColumnFamilyBlobIndex: u8 = 0x10; // Blob DB only
pub const kTypeRangeDeletion: u8 = 0xF;
pub const kTypeBlobIndex: u8 = 0x11; // Blob DB only
pub const kMaxValue: u8 = 0x7F; // Not used for storing records.

pub const kValueTypeForSeek: u8 = kTypeBlobIndex;

pub fn pack_sequence_and_type(seq: u64, t: u8) -> u64 {
    return (seq << 8) | t;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

pub const NullBlockHandle: BlockHandle = BlockHandle { offset: 0, size: 0 };

#[derive(Default, Debug, Clone, Copy)]
pub struct IndexValue {
    pub handle: BlockHandle,
    pub first_internal_key: Vec<u8>,
}

pub struct IndexValueRef<'a> {
    pub handle: BlockHandle,
    pub first_internal_key: &'a [u8],
}

impl<'a> IndexValueRef<'a> {
    pub fn new(handle: BlockHandle, first_internal_key: &'a [u8]) -> Self {
        Self {
            handle,
            first_internal_key,
        }
    }

    pub fn to_owned(&self) -> IndexValue {
        IndexValue {
            handle: self.handle.clone(),
            first_internal_key: self.first_internal_key.to_vec(),
        }
    }

    pub fn encode_to(&self, buff: &mut Vec<u8>, have_first_key: bool) {}
}

impl IndexValue {
    pub fn as_ref(&self) -> IndexValueRef {
        IndexValueRef {
            handle: self.handle.clone(),
            first_internal_key: self.first_internal_key.as_slice(),
        }
    }
}

pub fn extract_internal_key_footer(key: &[u8]) -> u64 {
    let l = key.len();
    assert!(key.len() >= 8);
    unsafe { u64::from_le_bytes(*(key as *const _ as *const [u8; 8])) }
}

pub fn extract_value_type(value: &[u8]) -> u8 {
    let num = extract_internal_key_footer(value);
    (num & 0xffu64) as u8
}

pub fn is_value_type(t: u8) -> bool {
    t <= kTypeMerge || t == kTypeBlobIndex
}

pub fn is_extended_value_type(t: u8) -> bool {
    t <= kTypeMerge || t == kTypeBlobIndex || t == kTypeRangeDeletion
}
