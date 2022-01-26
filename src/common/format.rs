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
