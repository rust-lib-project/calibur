use crate::util::encode_var_uint64;

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

pub const VALUE_TYPE_FOR_SEEK: u8 = ValueType::TypeBlobIndex as u8;

pub fn pack_sequence_and_type(seq: u64, t: u8) -> u64 {
    return (seq << 8) | t as u64;
}

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
}

pub const NullBlockHandle: BlockHandle = BlockHandle { offset: 0, size: 0 };

#[derive(Default, Debug, Clone)]
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
