mod error;
mod file;
mod file_system;
pub mod format;
pub mod options;
mod slice_transform;
mod snapshot;
pub use file::*;

use crate::util::{decode_fixed_uint64, extract_user_key};
pub use slice_transform::{InternalKeySliceTransform, SliceTransform};

pub use error::Error;
pub use file_system::{
    AsyncFileSystem, FileSystem, IOOption, InMemFileSystem, RandomAccessFile,
    RandomAccessFileReader, SequentialFile, SequentialFileReader, SyncPoxisFileSystem,
    WritableFile, WritableFileWriter,
};
pub type Result<T> = std::result::Result<T, Error>;

use crate::common::format::VALUE_TYPE_FOR_SEEK;
use std::cmp::Ordering;
use std::sync::Arc;

pub const MAX_SEQUENCE_NUMBER: u64 = (1u64 << 56) - 1;
pub const DISABLE_GLOBAL_SEQUENCE_NUMBER: u64 = u64::MAX;

pub trait KeyComparator: Send + Sync {
    fn name(&self) -> &str;
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn less_than(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        self.compare_key(lhs, rhs) == Ordering::Less
    }
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        self.compare_key(lhs, rhs) == Ordering::Equal
    }
    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]);
    fn find_short_successor(&self, key: &mut Vec<u8>) {
        // Find first character that can be incremented
        let n = key.len();
        for i in 0..n {
            let byte = key[i];
            if byte != 0xff {
                key[i] = byte + 1;
                key.resize(i + 1, 0);
                return;
            }
        }
        // *key is a run of 0xffs.  Leave it alone.
    }
}

#[derive(Default, Clone)]
pub struct DefaultUserComparator {}

impl KeyComparator for DefaultUserComparator {
    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs.eq(rhs)
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        let l = std::cmp::min(start.len(), limit.len());
        let mut diff_index = 0;
        while diff_index < l && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }
        if diff_index < l {
            let start_byte = start[diff_index];
            let limit_byte = limit[diff_index];
            if start_byte >= limit_byte {
                return;
            }
            if diff_index + 1 < limit.len() || start_byte + 1 < limit_byte {
                start[diff_index] += 1;
                start.resize(diff_index + 1, 0);
            } else {
                diff_index += 1;
                while diff_index < start.len() {
                    if start[diff_index] < 0xffu8 {
                        start[diff_index] += 1;
                        start.resize(diff_index + 1, 0);
                        break;
                    }
                    diff_index += 1;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct InternalKeyComparator {
    user_comparator: Arc<dyn KeyComparator>,
    name: String,
}

impl Default for InternalKeyComparator {
    fn default() -> Self {
        InternalKeyComparator::new(Arc::new(DefaultUserComparator::default()))
    }
}

impl InternalKeyComparator {
    pub fn new(user_comparator: Arc<dyn KeyComparator>) -> InternalKeyComparator {
        let mut name = "rocksdb.InternalKeyComparator:".to_string();
        name.push_str(user_comparator.name());
        InternalKeyComparator {
            user_comparator,
            name,
        }
    }

    pub fn get_user_comparator(&self) -> &Arc<dyn KeyComparator> {
        &self.user_comparator
    }
}

impl KeyComparator for InternalKeyComparator {
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let mut ret = self
            .user_comparator
            .compare_key(extract_user_key(lhs), extract_user_key(rhs));
        if ret == Ordering::Equal {
            let l = lhs.len() - 8;
            let r = rhs.len() - 8;
            let anum = decode_fixed_uint64(&lhs[l..]);
            let bnum = decode_fixed_uint64(&rhs[r..]);
            if anum > bnum {
                ret = Ordering::Less;
            } else {
                ret = Ordering::Greater;
            }
        }
        ret
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        let user_start = extract_user_key(&start);
        let user_limit = extract_user_key(limit);
        let mut tmp = user_start.to_vec();
        self.user_comparator
            .find_shortest_separator(&mut tmp, user_limit);
        if tmp.len() <= user_start.len()
            && self.user_comparator.compare_key(user_start, &tmp) == Ordering::Less
        {
            tmp.extend_from_slice(
                &format::pack_sequence_and_type(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK)
                    .to_le_bytes(),
            );
            std::mem::swap(start, &mut tmp);
        }
    }
    fn find_short_successor(&self, key: &mut Vec<u8>) {
        let user_key = extract_user_key(&key);
        let mut tmp = user_key.to_vec();
        self.user_comparator.find_short_successor(&mut tmp);
        if tmp.len() <= user_key.len()
            && self.user_comparator.compare_key(user_key, &tmp) == Ordering::Less
        {
            tmp.extend_from_slice(
                &format::pack_sequence_and_type(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK)
                    .to_le_bytes(),
            );
            std::mem::swap(key, &mut tmp);
        }
    }
}
