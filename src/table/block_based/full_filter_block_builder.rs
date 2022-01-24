use crate::common::format::BlockHandle;
use crate::common::SliceTransform;
use crate::table::block_based::filter_block_builder::*;
use crate::util::hash::bloom_hash;

const CACHE_LINE_SIZE: u32 = 128;

pub struct FullFilterBitsBuilder {
    hash_entries: Vec<u32>,
    bits_per_key: usize,
    num_probes: usize,
}

impl FullFilterBitsBuilder {
    fn add_key(&mut self, key: &[u8]) {
        let h = bloom_hash(key);
        if self.hash_entries.last().map_or(true, |e| *e != h) {
            self.hash_entries.push(h);
        }
    }

    fn calculate_space(&self, num_entry: usize) -> (Vec<u8>, u32, u32) {
        if num_entry > 0 {
            let total_bits_tmp = num_entry as u32 * self.bits_per_key as u32;
            let mut num_lines = (total_bits_tmp + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);
            if num_lines % 2 == 0 {
                num_lines += 1;
            }
            let total_bits = num_lines * (CACHE_LINE_SIZE * 8);
            let mut data = vec![0u8; total_bits as usize / 8 + 5];
            (data, total_bits, num_lines)
        } else {
            (vec![0u8; 5], 0, 0)
        }
    }

    fn add_hash(&self, mut h: u32, data: &mut Vec<u8>, num_lines: u32) {
        let delta = (h >> 17) | (h << 15);
        let b = (h % num_lines) * (CACHE_LINE_SIZE * 8);
        for i in 0..self.num_probes {
            let bitpos = b + (h % (CACHE_LINE_SIZE * 8));
            data[bitpos / 8] |= (1 << (bitpos % 8));
            h += delta;
        }
    }

    fn finish(&mut self) -> Vec<u8> {
        let (mut data, total_bits, num_lines) = self.calculate_space(self.hash_entries.len());
        if total_bits != 0 && num_lines != 0 {
            for h in self.hash_entries.drain(..) {
                self.add_hash(h, &mut data, num_lines);
            }
        }
        let pos = total_bits as usize / 8;
        data[pos] = self.num_probes as u8;
        data[pos + 1..].copy_from_slice(&num_lines.to_le_bytes());
        data
    }
}

pub struct FullFilterBlockBuilder {
    prefix_extractor: Option<Box<dyn SliceTransform>>,
    filter_bits_builder: FullFilterBitsBuilder,
    whole_key_filtering: bool,
    last_whole_key_recorded: bool,
    last_prefix_recorded: bool,
    last_whole_key_str: Vec<u8>,
    last_prefix_str: Vec<u8>,
    filter_data: Vec<u8>,
    num_added: u32,
}

impl FullFilterBlockBuilder {
    fn add_prefix(&mut self, key: &[u8]) {
        let prefix_extractor = self.prefix_extractor.take().unwrap();
        let prefix = prefix_extractor.transform(key);
        if self.whole_key_filtering {
            let last_prefix = self.last_prefix_str.as_slice();
            if !self.last_prefix_recorded || last_prefix.eq(prefix) {
                drop(last_prefix);
                self.add_key(prefix);
                self.last_prefix_recorded = true;
                self.last_prefix_str = prefix.to_vec();
            }
        } else {
            self.add_key(prefix);
        }
        self.prefix_extractor = Some(prefix_extractor);
    }

    fn add_key(&mut self, key: &[u8]) {
        self.filter_bits_builder.add_key(key);
        self.num_added += 1;
    }

    fn reset(&mut self) {
        self.last_whole_key_recorded = false;
        self.last_prefix_recorded = false;
    }
}

impl FilterBlockBuilder for FullFilterBlockBuilder {
    fn add(&mut self, key: &[u8]) {
        let add_prefix = self
            .prefix_extractor
            .as_ref()
            .map_or(false, |extractor| extractor.in_domain(key));
        if self.whole_key_filtering {
            if !add_prefix {
                self.add_key(key);
            } else {
                let last_whole_key = self.last_whole_key_str.as_slice();
                if !self.last_whole_key_recorded || !last_whole_key.eq(key) {
                    // drop(last_whole_key);
                    self.add_key(key);
                    self.last_whole_key_recorded = true;
                    self.last_whole_key_str = key.to_vec();
                }
            }
        }
        if add_prefix {
            self.add_prefix(key);
        }
    }

    fn start_block(&mut self, _: u64) {}

    fn finish(&mut self, _: &BlockHandle) -> crate::common::Result<&[u8]> {
        if self.num_added != 0 {
            self.filter_data = self.filter_bits_builder.finish();
            return Ok(&self.filter_data);
        }
        self.filter_data.clear();
        Ok(&self.filter_data)
    }

    fn num_added(&self) -> usize {
        self.num_added as usize
    }
}
