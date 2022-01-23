pub enum ValueType {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1,
    kTypeMerge = 0x2,
    kTypeLogData = 0x3,              // WAL only.
    kTypeColumnFamilyDeletion = 0x4, // WAL only.
    kTypeColumnFamilyValue = 0x5,    // WAL only.
    kTypeColumnFamilyMerge = 0x6,    // WAL only.
    kTypeSingleDeletion = 0x7,
    kTypeColumnFamilySingleDeletion = 0x8, // WAL only.
    kTypeBeginPrepareXID = 0x9,            // WAL only.
    kTypeEndPrepareXID = 0xA,              // WAL only.
    kTypeCommitXID = 0xB,                  // WAL only.
    kTypeRollbackXID = 0xC,                // WAL only.
    kTypeNoop = 0xD,                       // WAL only.
    kTypeColumnFamilyRangeDeletion = 0xE,  // WAL only.
    kTypeRangeDeletion = 0xF,              // meta block
    kTypeColumnFamilyBlobIndex = 0x10,     // Blob DB only
    kTypeBlobIndex = 0x11,                 // Blob DB only
    // When the prepared record is also persisted in db, we use a different
    // record. This is to ensure that the WAL that is generated by a WritePolicy
    // is not mistakenly read by another, which would result into data
    // inconsistency.
    kTypeBeginPersistedPrepareXID = 0x12, // WAL only.
    // Similar to kTypeBeginPersistedPrepareXID, this is to ensure that WAL
    // generated by WriteUnprepared write policy is not mistakenly read by
    // another.
    kTypeBeginUnprepareXID = 0x13, // WAL only.
    kMaxValue = 0x7F,              // Not used for storing records.
}

pub const kValueTypeForSeek: u8 = ValueType::kTypeBlobIndex as u8;

pub fn pack_sequence_and_type(seq: u64, t: u8) -> u64 {
    return (seq << 8) | t as u64;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
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
    let l = key.len();
    assert!(key.len() >= 8);
    unsafe { u64::from_le_bytes(*(key as *const _ as *const [u8; 8])) }
}

pub fn extract_value_type(value: &[u8]) -> u8 {
    let num = extract_internal_key_footer(value);
    (num & 0xffu64) as u8
}

pub fn is_value_type(t: u8) -> bool {
    t <= ValueType::kTypeMerge as u8 || t == ValueType::kTypeBlobIndex as u8
}

pub fn is_extended_value_type(t: u8) -> bool {
    t <= ValueType::kTypeMerge as u8
        || t == ValueType::kTypeBlobIndex as u8
        || t == ValueType::kTypeRangeDeletion as u8
}