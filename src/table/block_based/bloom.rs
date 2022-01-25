use crate::table::block_based::filter_block_builder::FilterPolicy;
use crate::util::hash::bloom_hash;

pub struct BloomFilterPolicy {
    bits_per_key: usize,
    num_probes: usize,
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        todo!()
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        false
    }

    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        // Compute bloom filter size (in both bits and bytes)
        let n = keys.len();
        let mut bits = n * self.bits_per_key;

        // For small n, we can see a very high false positive rate.  Fix it
        // by enforcing a minimum bloom filter length.
        if bits < 64 {
            bits = 64;
        }

        let mut bytes = (bits + 7) / 8;
        bits = bytes * 8;

        let init_size = dst.len();
        dst.resize(init_size + bytes, 0);
        dst.push(self.num_probes as u8); // Remember # of probes
        for &key in keys.iter() {
            // Use double-hashing to generate a sequence of hash values.
            // See analysis in [Kirsch,Mitzenmacher 2006].
            let mut h = bloom_hash(key);
            let delta = (h >> 17) | (h << 15); // Rotate right 17 bits
            for _ in 0..self.num_probes {
                let bitpos = h % bits as u32;
                dst[init_size + bitpos as usize / 8] |= (1 << (bitpos % 8)) as u8;
                h += delta;
            }
        }
    }
}
