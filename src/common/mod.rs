mod error;
mod file_system;
pub mod format;
pub mod options;

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

pub fn encode_var_uint32(data: &mut [u8], n: u32) -> usize {
    const B: u32 = 128;
    const MASK: u32 = 255;
    if n < (1 << 7) {
        data[0] = n as u8;
        return 1;
    } else if n < (1 << 14) {
        data[0] = ((n | B) & MASK) as u8;
        data[1] = (n >> 7) as u8;
        return 2;
    } else if n < (1 << 21) {
        data[0] = ((n | B) & MASK) as u8;
        data[1] = ((n >> 7 | B) & MASK) as u8;
        data[2] = (n >> 14) as u8;
        return 3;
    } else if n < (1 << 28) {
        data[0] = ((n | B) & MASK) as u8;
        data[1] = ((n >> 7 | B) & MASK) as u8;
        data[2] = ((n >> 14 | B) & MASK) as u8;
        data[3] = (n >> 21) as u8;
        return 4;
    } else {
        data[0] = ((n | B) & MASK) as u8;
        data[1] = ((n >> 7 | B) & MASK) as u8;
        data[2] = ((n >> 14 | B) & MASK) as u8;
        data[3] = ((n >> 21 | B) & MASK) as u8;
        data[4] = (n >> 28) as u8;
        return 5;
    }
}

pub fn get_var_uint32(data: &[u8]) -> Option<(usize, u32)> {
    const B: u8 = 128;
    const MASK: u32 = 127;
    if (data[0] & B) == 0 {
        return Some((1, data[0] as u32));
    }
    let mut ret: u32 = 0;
    for i in 0..5 {
        if i > data.len() {
            return None;
        }
        if (data[i] & B) > 0 {
            ret |= (data[i] as u32 & MASK) << (i as u32 * 7);
        } else {
            ret |= (data[i] as u32) << (i as u32 * 7);
            return Some((i + 1, ret));
        }
    }
    return None;
}

pub fn decode_fixed_uint32(array: &[u8]) -> u32 {
    ((array[0] as u32) << 0)
        + ((array[1] as u32) << 8)
        + ((array[2] as u32) << 16)
        + ((array[3] as u32) << 24)
}

pub fn difference_offset(origin: &[u8], target: &[u8]) -> usize {
    let mut off = 0;
    let len = std::cmp::min(origin.len(), target.len());
    while off < len && origin[off] == target[off] {
        off += 1;
    }
    off
}

pub fn extract_user_key(key: &[u8]) -> &[u8] {
    let l = key.len();
    &key[..(l - 8)]
}

pub fn hash(data: &[u8], seed: u32) -> u32 {
    const M: u32 = 0xc6a4a793;
    const R: u32 = 24;
    let mut h = seed ^ (data.len() as u32 * M);

    // Pick up four bytes at a time
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let w = decode_fixed_uint32(&data[offset..]);
        offset += 4;
        h += w;
        h *= M;
        h ^= h >> 16;
    }

    let rest = data.len() - offset;

    // Pick up remaining bytes
    if rest <= 3 {
        // Note: The original hash implementation used data[i] << shift, which
        // promotes the char to int and then performs the shift. If the char is
        // negative, the shift is undefined behavior in C++. The hash algorithm is
        // part of the format definition, so we cannot change it; to obtain the same
        // behavior in a legal way we just cast to uint32_t, which will do
        // sign-extension. To guarantee compatibility with architectures where chars
        // are unsigned we first cast the char to int8_t.
        h += (data[offset + 2] as u32) << 16;
    }
    if rest <= 2 {
        h += (data[offset + 1] as u32) << 8;
    }
    if rest <= 1 {
        h += data[offset] as u32;
        h *= M;
        h ^= h >> R;
    }
    return h;
}

pub fn key_hash(key: &[u8]) -> u32 {
    hash(key, 397)
}
