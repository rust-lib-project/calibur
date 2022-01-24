mod error;
mod file_system;
pub mod format;
pub mod options;
mod slice_transform;
use crate::util::extract_user_key;
pub use slice_transform::SliceTransform;

pub use error::Error;
pub use file_system::{
    FileSystem, PosixWritableFile, RandomAccessFileReader, WritableFile, WritableFileWriter,
};
pub type Result<T> = std::result::Result<T, Error>;
use bytes::Bytes;

use crate::common::format::kValueTypeForSeek;
use std::cmp::Ordering;
const MaxSequenceNumber: u64 = (1u64 << 56) - 1;

pub trait KeyComparator: Clone {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool;
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

#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultUserComparator {}

impl KeyComparator for DefaultUserComparator {
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

#[derive(Default, Debug, Clone, Copy)]
pub struct FixedLengthSuffixComparator {
    user_comparator: DefaultUserComparator,
    len: usize,
}

impl FixedLengthSuffixComparator {
    pub fn new(len: usize) -> FixedLengthSuffixComparator {
        FixedLengthSuffixComparator {
            user_comparator: DefaultUserComparator::default(),
            len,
        }
    }

    pub fn get_user_comparator(&self) -> &DefaultUserComparator {
        &self.user_comparator
    }
}

impl KeyComparator for FixedLengthSuffixComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        if lhs.len() < self.len {
            panic!(
                "cannot compare with suffix {}: {:?}",
                self.len,
                Bytes::copy_from_slice(lhs)
            );
        }
        if rhs.len() < self.len {
            panic!(
                "cannot compare with suffix {}: {:?}",
                self.len,
                Bytes::copy_from_slice(rhs)
            );
        }
        let (l_p, l_s) = lhs.split_at(lhs.len() - self.len);
        let (r_p, r_s) = rhs.split_at(rhs.len() - self.len);
        let res = l_p.cmp(r_p);
        match res {
            Ordering::Greater | Ordering::Less => res,
            Ordering::Equal => l_s.cmp(r_s),
        }
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        let (l_p, _) = lhs.split_at(lhs.len() - self.len);
        let (r_p, _) = rhs.split_at(rhs.len() - self.len);
        l_p == r_p
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
                &format::pack_sequence_and_type(MaxSequenceNumber, kValueTypeForSeek).to_le_bytes(),
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
                &format::pack_sequence_and_type(MaxSequenceNumber, kValueTypeForSeek).to_le_bytes(),
            );
            std::mem::swap(key, &mut tmp);
        }
    }
}
