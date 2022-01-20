mod error;
mod file_system;
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
