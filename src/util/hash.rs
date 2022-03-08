use super::decode_fixed_uint32;

pub fn hash(data: &[u8], seed: u64) -> u32 {
    const M: u32 = 0xc6a4a793;
    const R: u32 = 24;
    let mut h = (seed ^ (data.len() as u64 * M as u64)) as u32;

    // Pick up four bytes at a time
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let w = decode_fixed_uint32(&data[offset..]);
        offset += 4;
        h = h.wrapping_add(w);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }

    let rest = data.len() - offset;

    // Pick up remaining bytes
    if rest >= 3 {
        // Note: The original hash implementation used data[i] << shift, which
        // promotes the char to int and then performs the shift. If the char is
        // negative, the shift is undefined behavior in C++. The hash algorithm is
        // part of the format definition, so we cannot change it; to obtain the same
        // behavior in a legal way we just cast to uint32_t, which will do
        // sign-extension. To guarantee compatibility with architectures where chars
        // are unsigned we first cast the char to int8_t.
        h += (data[offset + 2] as u32) << 16;
    }
    if rest >= 2 {
        h += (data[offset + 1] as u32) << 8;
    }
    if rest >= 1 {
        h += data[offset] as u32;
        h = h.wrapping_mul(M);
        h ^= h >> R;
    }
    h
}

pub fn key_hash(key: &[u8]) -> u32 {
    hash(key, 397)
}

pub fn bloom_hash(key: &[u8]) -> u32 {
    hash(key, 0xbc9f1d34)
}
