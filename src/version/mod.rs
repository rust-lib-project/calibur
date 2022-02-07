mod column_family;
pub mod snapshot;
mod version;
mod version_set;
pub mod version_storage_info;

use crate::common::MAX_SEQUENCE_NUMBER;
use crate::util::{
    put_length_prefixed_slice, put_var_uint32, put_var_uint64, put_varint32varint32,
    put_varint32varint32varint64, put_varint32varint64, put_varint64varint64, BtreeComparable,
};
use bytes::Bytes;
pub use column_family::ColumnFamily;
pub use version::*;
pub use version_set::{VersionSet, VersionSetKernel};

const FILE_NUMBER_MASK: u64 = 0x3FFFFFFFFFFFFFFF;

pub fn pack_file_number_and_path_id(number: u64, path_id: u64) -> u64 {
    number | (path_id * (FILE_NUMBER_MASK + 1))
}

#[derive(Clone, Default)]
pub struct FileDescriptor {
    pub file_size: u64,
    pub packed_number_and_path_id: u64,
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
}

impl FileDescriptor {
    pub fn new(id: u64, path_id: u32) -> Self {
        Self {
            file_size: 0,
            packed_number_and_path_id: pack_file_number_and_path_id(id, path_id as u64),
            smallest_seqno: MAX_SEQUENCE_NUMBER,
            largest_seqno: 0,
        }
    }

    pub fn get_number(&self) -> u64 {
        self.packed_number_and_path_id & FILE_NUMBER_MASK
    }

    pub fn get_path_id(&self) -> u32 {
        (self.packed_number_and_path_id / (FILE_NUMBER_MASK + 1)) as u32
    }
}

#[derive(Clone)]
pub struct FileMetaData {
    pub fd: FileDescriptor,
    pub level: u32,
    pub smallest: Bytes,
    pub largest: Bytes,
    pub being_compact: bool,
    pub marked_for_compaction: bool,
}

impl FileMetaData {
    pub fn new(id: u64, level: u32, smallest: Vec<u8>, largest: Vec<u8>) -> Self {
        FileMetaData {
            fd: FileDescriptor::new(id, 0),
            level,
            smallest: Bytes::from(smallest),
            largest: Bytes::from(largest),
            being_compact: false,
            marked_for_compaction: false,
        }
    }

    pub fn update_boundary(&mut self, key: &[u8], seqno: u64) {
        if self.smallest.is_empty() {
            self.smallest = key.to_vec().into();
        }
        self.largest = Bytes::from(key.to_vec());
        self.fd.smallest_seqno = std::cmp::min(self.fd.smallest_seqno, seqno);
        self.fd.largest_seqno = std::cmp::max(self.fd.largest_seqno, seqno);
    }
}

impl BtreeComparable for FileMetaData {
    fn smallest(&self) -> &Bytes {
        &self.smallest
    }

    fn largest(&self) -> &Bytes {
        &self.largest
    }

    fn id(&self) -> u64 {
        self.fd.get_number()
    }
}

#[derive(Default, Clone)]
pub struct VersionEdit {
    pub add_files: Vec<FileMetaData>,
    pub deleted_files: Vec<FileMetaData>,

    // memtable to be deleted does not need to be persisted in manifest
    pub mems_deleted: Vec<u64>,

    pub max_level: u32,
    pub comparator: String,
    pub log_number: u64,
    pub prev_log_number: u64,
    pub next_file_number: u64,
    pub max_column_family: u64,
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
}

impl VersionEdit {
    pub fn encode_to(&self, buf: &mut Vec<u8>) -> bool {
        if self.has_comparator {
            put_var_uint32(buf, Tag::Comparator as u32);
            put_length_prefixed_slice(buf, self.comparator.as_bytes());
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
            put_varint32varint64(buf, Tag::MaxColumnFamily as u32, self.max_column_family);
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
            put_varint32varint32(buf, Tag::ColumnFamilyAdd as u32, self.column_family);
        }
        if self.is_column_family_drop {
            put_var_uint32(buf, Tag::ColumnFamilyDrop as u32);
        }
        // TODO: support atomic group
        true
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
}
