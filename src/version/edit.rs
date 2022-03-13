use super::FileMetaData;
use crate::common::{Error, Result};
use crate::util::{
    get_length_prefixed_slice, get_var_uint32, get_var_uint64, put_length_prefixed_slice,
    put_var_uint32, put_var_uint64, put_varint32varint32, put_varint32varint32varint64,
    put_varint32varint64, put_varint64varint64,
};
use crate::ColumnFamilyOptions;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Default, Eq, PartialEq)]
pub struct ColumnFamilyOptionsWrapper {
    pub options: Option<ColumnFamilyOptions>,
}

impl Debug for ColumnFamilyOptionsWrapper {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct VersionEdit {
    pub add_files: Vec<FileMetaData>,
    pub deleted_files: Vec<FileMetaData>,

    // only for create column family in manual
    pub cf_options: ColumnFamilyOptionsWrapper,

    pub max_level: u32,
    pub comparator_name: String,
    pub log_number: u64,
    pub prev_log_number: u64,
    pub next_file_number: u64,
    pub max_column_family: u32,
    pub min_log_number_to_keep: u64,
    pub last_sequence: u64,

    pub has_comparator: bool,
    pub has_log_number: bool,
    pub has_prev_log_number: bool,
    pub has_next_file_number: bool,
    pub has_last_sequence: bool,
    pub has_max_column_family: bool,
    pub has_min_log_number_to_keep: bool,

    pub is_column_family_drop: bool,
    pub is_column_family_add: bool,
    pub column_family: u32,
    pub column_family_name: String,
}

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
#[repr(u32)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum Tag {
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    // 8 was used for large value refs
    PrevLogNumber = 9,
    MinLogNumberToKeep = 10,

    // these are new formats divergent from open source leveldb
    NewFile2 = 100,
    NewFile3 = 102,
    NewFile4 = 103,     // 4th (the latest) format version of adding files
    ColumnFamily = 200, // specify column family for version edit
    ColumnFamilyAdd = 201,
    ColumnFamilyDrop = 202,
    MaxColumnFamily = 203,

    InAtomicGroup = 300,
    Unknown = 65535,
}

impl From<u32> for Tag {
    fn from(x: u32) -> Self {
        if x == 0 || (x > 10 && x < 100) || (x > 103 && x < 200) || x > 203 {
            Tag::Unknown
        } else {
            unsafe { std::mem::transmute(x) }
        }
    }
}
impl VersionEdit {
    pub fn encode_to(&self, buf: &mut Vec<u8>) -> bool {
        if self.has_comparator {
            put_var_uint32(buf, Tag::Comparator as u32);
            put_length_prefixed_slice(buf, self.comparator_name.as_bytes());
        }
        if self.has_log_number {
            put_varint32varint64(buf, Tag::LogNumber as u32, self.log_number);
        }
        if self.has_prev_log_number {
            put_varint32varint64(buf, Tag::PrevLogNumber as u32, self.prev_log_number);
        }
        if self.has_next_file_number {
            put_varint32varint64(buf, Tag::NextFileNumber as u32, self.next_file_number);
        }
        if self.has_last_sequence {
            put_varint32varint64(buf, Tag::LastSequence as u32, self.last_sequence);
        }
        if self.has_max_column_family {
            put_varint32varint32(buf, Tag::MaxColumnFamily as u32, self.max_column_family);
        }
        for f in &self.deleted_files {
            put_varint32varint32varint64(buf, Tag::DeletedFile as u32, f.level, f.id());
        }
        // let mut min_log_num_written = false;
        for f in &self.add_files {
            let mut has_customized_fields = false;
            if f.marked_for_compaction || self.has_min_log_number_to_keep {
                put_var_uint32(buf, Tag::NewFile4 as u32);
                has_customized_fields = true;
            } else if f.fd.get_path_id() == 0 {
                put_var_uint32(buf, Tag::NewFile2 as u32);
            } else {
                put_var_uint32(buf, Tag::NewFile3 as u32);
            }
            put_varint32varint64(buf, f.level, f.fd.get_number());
            if f.fd.get_path_id() != 0 && !has_customized_fields {
                put_var_uint32(buf, f.fd.get_path_id());
            }
            put_var_uint64(buf, f.fd.file_size);
            put_length_prefixed_slice(buf, f.smallest.as_ref());
            put_length_prefixed_slice(buf, f.largest.as_ref());
            put_varint64varint64(buf, f.fd.smallest_seqno, f.fd.largest_seqno);
            // TODO: support customized fields.
        }
        if self.column_family != 0 {
            put_varint32varint32(buf, Tag::ColumnFamily as u32, self.column_family);
        }
        if self.is_column_family_add {
            put_var_uint32(buf, Tag::ColumnFamilyAdd as u32);
            put_length_prefixed_slice(buf, self.column_family_name.as_bytes());
        }
        if self.is_column_family_drop {
            put_var_uint32(buf, Tag::ColumnFamilyDrop as u32);
        }
        // TODO: support atomic group
        true
    }

    pub fn set_log_number(&mut self, log_number: u64) {
        self.log_number = log_number;
        self.has_log_number = true;
    }

    pub fn add_column_family(&mut self, name: String) {
        self.is_column_family_add = true;
        self.column_family_name = name;
    }

    pub fn set_comparator_name(&mut self, name: &str) {
        self.has_comparator = true;
        self.comparator_name = name.to_string();
    }

    pub fn set_next_file(&mut self, file_number: u64) {
        self.next_file_number = file_number;
        self.has_next_file_number = true;
    }

    pub fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = seq;
        self.has_last_sequence = true;
    }

    pub fn set_max_column_family(&mut self, c: u32) {
        self.has_max_column_family = true;
        self.max_column_family = c;
    }

    pub fn get_log_number(&self) -> u64 {
        self.log_number
    }

    pub fn decode_from(&mut self, src: &[u8]) -> Result<()> {
        let mut offset = 0;
        let mut err_msg: &'static str = "";
        while let Some(tag_val) = get_var_uint32(&src[offset..], &mut offset) {
            let tag = tag_val.into();
            match tag {
                Tag::Comparator => match get_length_prefixed_slice(&src[offset..], &mut offset) {
                    Some(data) => {
                        self.comparator_name = String::from_utf8(data.to_vec())
                            .map_err(|_| Error::VarDecode("decode comparator error"))?;
                    }
                    None => {
                        err_msg = "comparator name";
                        break;
                    }
                },
                Tag::LogNumber => {
                    self.log_number = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("log number"))?;
                    self.has_log_number = true;
                }
                Tag::NextFileNumber => {
                    self.next_file_number = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("next file number"))?;
                    self.has_next_file_number = true;
                }
                Tag::LastSequence => {
                    self.last_sequence = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("last sequence"))?;
                    self.has_last_sequence = true;
                }
                Tag::CompactPointer => {
                    err_msg = "do not support upgrade from compact pointer";
                    break;
                }
                Tag::DeletedFile => {
                    let level = get_var_uint32(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("deleted file"))?;
                    if level > self.max_level {
                        self.max_level = level;
                    }
                    let val = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("deleted file"))?;
                    self.deleted_files
                        .push(FileMetaData::new(val, level, vec![], vec![]));
                }
                Tag::NewFile => {
                    let level = get_var_uint32(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file"))?;
                    if level > self.max_level {
                        self.max_level = level;
                    }
                    let file_number = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file"))?;
                    let file_size = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file"))?;
                    let smallest = get_length_prefixed_slice(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file"))?;
                    let largest = get_length_prefixed_slice(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file"))?;
                    let mut f =
                        FileMetaData::new(file_number, level, smallest.to_vec(), largest.to_vec());
                    f.level = level;
                    f.fd.file_size = file_size;
                    self.add_files.push(f);
                }
                Tag::PrevLogNumber => {
                    self.prev_log_number = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("prev log number"))?;
                    self.has_prev_log_number = true;
                }
                Tag::MinLogNumberToKeep => {
                    self.min_log_number_to_keep = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("min log number to keep"))?;
                    self.has_prev_log_number = true;
                }
                Tag::NewFile2 => {
                    let level = get_var_uint32(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    if level > self.max_level {
                        self.max_level = level;
                    }
                    let file_number = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    let file_size = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    let smallest = get_length_prefixed_slice(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    let largest = get_length_prefixed_slice(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    let mut f =
                        FileMetaData::new(file_number, level, smallest.to_vec(), largest.to_vec());
                    f.level = level;
                    f.fd.file_size = file_size;
                    f.fd.smallest_seqno = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    f.fd.largest_seqno = get_var_uint64(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("new file3"))?;
                    self.add_files.push(f);
                }
                Tag::NewFile3 => {
                    err_msg = "do not support NewFiles3 sst";
                    break;
                }
                Tag::NewFile4 => {
                    err_msg = "do not support NewFiles4 sst";
                    break;
                }
                Tag::ColumnFamily => {
                    self.column_family = get_var_uint32(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("column family"))?;
                }
                Tag::ColumnFamilyAdd => {
                    let cf = get_length_prefixed_slice(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("column family add"))?;
                    self.column_family_name = String::from_utf8(cf.to_vec())
                        .map_err(|_| Error::VarDecode("column family add"))?;
                    self.is_column_family_add = true;
                }
                Tag::ColumnFamilyDrop => {
                    self.is_column_family_drop = true;
                }
                Tag::MaxColumnFamily => {
                    self.max_column_family = get_var_uint32(&src[offset..], &mut offset)
                        .ok_or(Error::VarDecode("column family"))?;
                    self.has_max_column_family = true;
                }
                Tag::InAtomicGroup => {
                    // TODO: support atomic group
                    err_msg = "do not support atomic group";
                    break;
                }
                Tag::Unknown => {
                    err_msg = "unknown tag, manifest may be corrupted";
                    break;
                }
            }
        }
        if !err_msg.is_empty() {
            return Err(Error::VarDecode(err_msg));
        }
        Ok(())
    }

    pub fn add_file(
        &mut self,
        level: u32,
        file_number: u64,
        file_size: u64,
        smallest: &[u8],
        largest: &[u8],
        smallest_seqno: u64,
        largest_seqno: u64,
    ) {
        let mut f = FileMetaData::new(file_number, level, smallest.to_vec(), largest.to_vec());
        f.fd.file_size = file_size;
        f.fd.smallest_seqno = smallest_seqno;
        f.fd.largest_seqno = largest_seqno;
        self.add_files.push(f);
    }

    pub fn delete_file(&mut self, level: u32, file_number: u64) {
        let f = FileMetaData::new(file_number, level, vec![], vec![]);
        self.deleted_files.push(f);
    }
}

#[cfg(test)]
mod tests {
    use super::{FileMetaData, VersionEdit};

    #[test]
    fn test_manifest_decode_encode() {
        let mut edit = VersionEdit::default();
        edit.column_family = 1;
        edit.log_number = 15;
        edit.has_log_number = true;

        for i in 0..5u64 {
            let mut smallest = b"abcd".to_vec();
            let mut largest = b"abcd".to_vec();
            smallest.extend_from_slice(&(i * 2).to_le_bytes());
            largest.extend_from_slice(&(i * 2 + 1).to_le_bytes());
            let mut f = FileMetaData::new(i + 1, 0, smallest, largest);
            f.fd.smallest_seqno = i * 100;
            f.fd.largest_seqno = i * 100 + 50;
            edit.add_files.push(f);
        }
        let f = FileMetaData::new(0, 0, vec![], vec![]);
        edit.deleted_files.push(f);
        let mut record = vec![];
        edit.encode_to(&mut record);
        let mut new_edit = VersionEdit::default();
        new_edit.decode_from(&record).unwrap();
        assert_eq!(edit, new_edit);
    }
}
