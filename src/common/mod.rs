mod error;
mod file_system;
pub use file_system::{WritableFileWriter, RandomAccessFileReader};
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;

use std::cmp::Ordering;

pub trait KeyComparator: Clone {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FixedLengthSuffixComparator {
    len: usize,
}

impl FixedLengthSuffixComparator {
    pub const fn new(len: usize) -> FixedLengthSuffixComparator {
        FixedLengthSuffixComparator { len }
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
}

pub fn encode_var_uint32(data: &mut [u8], n: u32) -> usize {
    let mut offset = 0;
    const B: u8 = 128;
    const MASK: u32 = 255;
    if n < (1 << 7) {
        data[0] = n as u8;
        return 1;
    } else if v < (1 << 14) {
        data[0] = ((n | B) & MASK) as u8;
        data[1] = (n >> 7) as u8;
        return 2;
    } else if (v < (1 << 21)) {
        data[0] = ((n | B) & MASK) as u8;
        data[1] = ((n >> 7 | B) & MASK) as u8;
        data[2] = (n >> 14) as u8;
        return 3;
    } else if (v < (1 << 28)) {
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
    const MASK: u8 = 127;
    if (data[0] & B) == 0 {
        return Some((1, data[0] as u32));
    }
    let mut ret: u32 = 0;
    for i in 0..5 {
        if i > data.len() {
            return None;
        }
        if (data[i] & B) > 0 {
            ret |= (data[i] & MASK) << (i * 7);
        } else {
            ret |= data[i] << (i * 7);
            return Some((i + 1, ret));
        }
    }
    return None;
}

pub fn decode_fixed_uint32(data: &[u8]) -> u32 {
}

pub fn difference_offset(origin: &[u8], target: &[u8]) -> usize {
    let mut off = 0;
    let len = std::cmp::min(origin.len(), target.len());
    while off < len && origin[off] == target[off] {}
    off
}

pub fn extract_user_key(key: &[u8]) -> &[u8] {
    let l = key.len();
    &key[..(l - 8)]
}

pub fn hash(data: &[u8], seed: u32) -> u32 {
    const m: u32 = 0xc6a4a793;
    const r: u32 = 24;
    let mut h = (seed ^ (data.len() as u32 * m));

    // Pick up four bytes at a time
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let w = decode_fixed_uint32(&data[offset..]);
        offset += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    let rest =  data.len() - offset;

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
        h += data[offset];
        h *= m;
        h ^= (h >> r);
    }
    return h;
}

pub fn key_hash(key: &[u8]) -> u32 {
    hash(key, 397)
}