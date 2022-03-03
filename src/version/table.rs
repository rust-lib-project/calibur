use crate::common::format::extract_user_key;
use crate::common::FileSystem;
use crate::common::MAX_SEQUENCE_NUMBER;
use crate::table::TableReader;
use crate::util::BtreeComparable;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

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
    pub marked_for_compaction: bool,
    pub num_entries: u64,
}

impl FileMetaData {
    pub fn new(id: u64, level: u32, smallest: Vec<u8>, largest: Vec<u8>) -> Self {
        FileMetaData {
            fd: FileDescriptor::new(id, 0),
            level,
            smallest: Bytes::from(smallest),
            largest: Bytes::from(largest),
            marked_for_compaction: false,
            num_entries: 0,
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

    pub fn id(&self) -> u64 {
        self.fd.get_number()
    }
}

pub struct TableFile {
    pub meta: FileMetaData,
    deleted: AtomicBool,
    pub reader: Box<dyn TableReader>,
    fs: Arc<dyn FileSystem>,
    smallest: Vec<u8>,
    largest: Vec<u8>,
    path: PathBuf,
    being_compact: AtomicBool,
}

impl TableFile {
    pub fn new(
        meta: FileMetaData,
        table: Box<dyn TableReader>,
        fs: Arc<dyn FileSystem>,
        path: PathBuf,
    ) -> Self {
        let smallest = extract_user_key(meta.smallest.as_ref()).to_vec();
        let largest = extract_user_key(meta.largest.as_ref()).to_vec();
        TableFile {
            path,
            fs,
            reader: table,
            meta,
            smallest,
            largest,
            deleted: AtomicBool::new(false),
            being_compact: AtomicBool::new(false),
        }
    }

    pub fn mark_removed(&self) {
        self.deleted.store(true, Ordering::Release);
    }

    pub fn mark_compaction(&self) {
        self.being_compact.store(true, Ordering::Release);
    }

    pub fn is_pending_compaction(&self) -> bool {
        self.being_compact.load(Ordering::Acquire)
    }
}

impl Drop for TableFile {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Acquire) {
            println!("delete file {:?}", self.path);
            let _ = self.fs.remove(self.path.clone());
            // TODO: log error here.
        }
    }
}

impl BtreeComparable for Arc<TableFile> {
    fn smallest(&self) -> &[u8] {
        &self.smallest
    }

    fn largest(&self) -> &[u8] {
        &self.largest
    }

    fn id(&self) -> u64 {
        self.meta.id()
    }
}
