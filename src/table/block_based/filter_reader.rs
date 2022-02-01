use crate::util::decode_fixed_uint32;
use crate::util::hash::bloom_hash;

pub trait FilterBlockReader: Sync + Send {
    fn is_block_based(&self) -> bool {
        false
    }
    fn key_may_match(&self, key: &[u8]) -> bool;
}

pub struct FullFilterBlockReader {
    data: Vec<u8>,
    num_probes: usize,
    num_lines: u32,
    log2_cache_line_size: u32,
}

impl FullFilterBlockReader {
    pub fn new(data: Vec<u8>) -> Self {
        let mut reader = FullFilterBlockReader {
            data,
            num_probes: 0,
            num_lines: 0,
            log2_cache_line_size: 0,
        };
        let l = reader.data.len();
        if l > 5 {
            reader.num_probes = reader.data[l - 5] as usize;
            reader.num_lines = decode_fixed_uint32(&reader.data[(l - 4)..]);
        }
        if reader.num_lines != 0 && (reader.data.len() - 5) % reader.num_lines as usize != 0 {
            reader.num_lines = 0;
            reader.num_probes = 0;
        } else if reader.num_lines != 0 {
            let mut num_lines_at_curr_cache_size =
                (reader.data.len() as u32 - 5) >> reader.log2_cache_line_size;
            while num_lines_at_curr_cache_size != reader.num_lines {
                reader.log2_cache_line_size += 1;
                num_lines_at_curr_cache_size =
                    (reader.data.len() as u32 - 5) >> reader.log2_cache_line_size;
            }
        }
        reader
    }

    fn hash_may_match(&self, mut h: u32, bit_offset: u32) -> bool {
        let delta = (h >> 17) | (h << 15);
        for _ in 0..self.num_probes {
            let bitpos = bit_offset + (h & ((1 << (self.log2_cache_line_size + 3)) - 1));
            if (self.data[bitpos as usize / 8] & (1u8 << (bitpos % 8) as u8)) == 0 {
                return false;
            }
            h += delta;
        }
        true
    }
}

impl FilterBlockReader for FullFilterBlockReader {
    fn key_may_match(&self, key: &[u8]) -> bool {
        let h = bloom_hash(key);
        // Left shift by an extra 3 to convert bytes to bits
        let bit_offset = (h % self.num_lines) << (self.log2_cache_line_size + 3);
        self.hash_may_match(h, bit_offset)
    }
}
