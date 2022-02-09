mod column_family;
mod edit;
pub mod snapshot;
mod version;
mod version_set;
pub mod version_storage_info;
pub use edit::VersionEdit;

use crate::common::MAX_SEQUENCE_NUMBER;
use crate::util::BtreeComparable;
use bytes::Bytes;
pub use column_family::ColumnFamily;
pub use version::*;
pub use version_set::{VersionSet, VersionSetKernel};

const FILE_NUMBER_MASK: u64 = 0x3FFFFFFFFFFFFFFF;

pub fn pack_file_number_and_path_id(number: u64, path_id: u64) -> u64 {
    number | (path_id * (FILE_NUMBER_MASK + 1))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
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
