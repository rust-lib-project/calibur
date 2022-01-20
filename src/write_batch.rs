use crate::common::{encode_var_uint32, get_var_int32, get_var_uint32};
use bytes::{BufMut, BytesMut};

const kTypeDeletion: u8 = 0x0;
const kTypeValue: u8 = 0x1;
const kTypeMerge: u8 = 0x2;
const kTypeColumnFamilyDeletion: u8 = 0x4; // WAL only.
const kTypeColumnFamilyValue: u8 = 0x5; // WAL only.
const kTypeColumnFamilyMerge: u8 = 0x6; // WAL only.
const kTypeColumnFamilyBlobIndex: u8 = 0x10; // Blob DB only
const kTypeBlobIndex: u8 = 0x11; // Blob DB only
const kMaxValue: u8 = 0x7F; // Not used for storing records.

const DEFERRED: u8 = 1 << 0;
const HAS_PUT: u8 = 1 << 1;
const HAS_DELETE: u8 = 1 << 2;
const HAS_SINGLE_DELETE: u8 = 1 << 3;
const HAS_MERGE: u8 = 1 << 4;
const HAS_BLOB_INDEX: u8 = 1 << 10;

pub struct WriteBatch {
    data: Vec<u8>,
    count: u32,
    flag: u32,
}

pub enum WriteBatchItem<'a> {
    Put {
        cf: u32,
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        cf: u32,
        key: &'a [u8],
    },
}

impl WriteBatch {
    pub fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) {
        let mut tmp: [u8; 5];
        self.count += 1;
        self.set_count(self.count);
        if cf == 0 {
            self.data.put(kTypeValue);
        } else {
            self.data.put(kTypeColumnFamilyValue);
            let offset = encode_var_uint32(&mut tmp, cf);
            self.data.extend_from_slice(&tmp[..offset]);
        }
        let offset = encode_var_uint32(&mut tmp, key.len() as u32);
        self.data.extend_from_slice(&tmp[..offset]);
        self.data.extend_from_slice(key);
        let offset = encode_var_uint32(&mut tmp, value.len() as u32);
        self.data.extend_from_slice(&tmp[..offset]);
        self.data.extend_from_slice(value);
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.put_cf(0, key, value);
    }

    pub fn delete_cf(&mut self, cf: u32, key: &[u8]) {
        let mut tmp: [u8; 5];
        self.count += 1;
        self.set_count(self.count);
        if cf == 0 {
            self.data.put(kTypeDeletion);
        } else {
            self.data.put(kTypeColumnFamilyDeletion);
            let offset = encode_var_uint32(&mut tmp, cf);
            self.data.extend_from_slice(&tmp[..offset]);
        }
        let offset = encode_var_uint32(&mut tmp, key.len() as u32);
        self.data.extend_from_slice(&tmp[..offset]);
        self.data.extend_from_slice(key);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.delete_cf(0, key);
    }

    fn set_count(&mut self, count: u32) {
        let c = count.to_be_bytes();
        self.data[8..].copy_from_slice(&c);
    }

    fn iter<'a>(&self) -> WriteBatchIter<'a> {
        WriteBatchIter {
            batch: self,
            offset: 0,
        }
    }
}

pub struct WriteBatchIter<'a> {
    batch: &'a WriteBatch,
    offset: usize,
}

impl<'a> WriteBatchIter<'a> {
    pub fn read_record<'a>(&mut self) -> Option<WriteBatchItem<'a>> {
        let tag = self.batch.data[self.offset];
        let mut cf = 0;
        self.offset += 1;
        if tag == kTypeColumnFamilyValue || tag == kTypeColumnFamilyDeletion {
            if let Some((l, cf_id)) = get_var_uint32(&self.batch.data[self.offset..]) {
                self.offset += l;
                cf = cf_id;
            } else {
                return None;
            }
            None
        }
        if tag == kTypeValue || tag == kTypeColumnFamilyValue {
            if let Some((l, key_len)) = get_var_uint32(&self.batch.data[self.offset..]) {
                self.offset += l;
                if self.offset + key_len > self.batch.data.len() {
                    return None;
                }
                let key_pos = self.offset;
                self.offset += key_len;
                let key = &self.batch.data[key_pos..self.offset];
                if let Some((l, value_len)) = get_var_uint32(&self.batch.data[self.offset..]) {
                    self.offset += l;
                    let v_pos = self.offset;
                    self.offset += value_len;
                    if self.offset > self.batch.data.len() {
                        return None;
                    }
                    return Some(WriteBatchItem::Put {
                        cf,
                        key,
                        value: &self.batch.data[v_pos..self.offset],
                    });
                }
            }
            return None;
        } else if tag == kTypeDeletion || tag == kTypeColumnFamilyDeletion {
            if let Some((l, key_len)) = get_var_uint32(&self.batch.data[self.offset..]) {
                self.offset += l;
                if self.offset + key_len > self.batch.data.len() {
                    return None;
                }
                let key_pos = self.offset;
                self.offset += key_len;
                let key = &self.batch.data[key_pos..self.offset];
                return Some(WriteBatchItem::Delete { cf, key });
            }

            return None;
        } else {
            None
        }
    }
}

impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = WriteBatchItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_record()
    }
}
