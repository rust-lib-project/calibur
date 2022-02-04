pub mod hash;
mod btree;

pub fn decode_fixed_uint32(key: &[u8]) -> u32 {
    unsafe { u32::from_le_bytes(*(key as *const _ as *const [u8; 4])) }
}

pub fn decode_fixed_uint16(key: &[u8]) -> u16 {
    unsafe { u16::from_le_bytes(*(key as *const _ as *const [u8; 2])) }
}

pub fn decode_fixed_uint64(key: &[u8]) -> u64 {
    unsafe { u64::from_le_bytes(*(key as *const _ as *const [u8; 8])) }
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

pub fn encode_var_uint64(data: &mut [u8], mut v: u64) -> usize {
    const B: u64 = 128;
    let mut offset = 0;
    while v >= B {
        data[offset] = ((v & (B - 1)) | B) as u8;
        v >>= 7u64;
        offset += 1;
    }
    data[offset] = v as u8;
    offset + 1
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

pub fn get_var_uint64(data: &[u8]) -> Option<(usize, u64)> {
    const B: u8 = 128;
    const MASK: u64 = 127;

    let mut ret: u64 = 0;
    let mut shift = 0;
    let mut offset = 0;
    while shift <= 63 && offset < data.len() {
        if data[offset] & B > 0 {
            ret |= (data[offset] as u64 & MASK) << shift;
        } else {
            ret |= (data[offset] as u64) << shift;
            return Some((offset + 1, ret));
        }
        shift += 7;
        offset += 1;
    }
    return None;
}

pub fn next_key(key: &mut Vec<u8>) {
    if *key.last().unwrap() < 255u8 {
        *key.last_mut().unwrap() += 1;
    } else {
        key.push(0);
    }
}

pub fn get_next_key(key: &[u8]) -> Vec<u8> {
    let mut data = key.to_vec();
    if *data.last().unwrap() < 255u8 {
        *data.last_mut().unwrap() += 1;
    } else {
        data.push(0);
    }
    data
}

const MASK_DELTA: u32 = 0xa282ead8u32;

pub fn crc_mask(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)) + MASK_DELTA
}
pub fn crc_unmask(masked_crc: u32) -> u32 {
    let rot = masked_crc - MASK_DELTA;
    (rot >> 17) | (rot << 15)
}
