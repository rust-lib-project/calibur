use crate::common::SliceTransform;
use crate::table::block_based::filter_block_builder::*;
use crate::table::block_based::filter_reader::{FilterBlockReader, FullFilterBlockReader};
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::util::hash::bloom_hash;
use std::sync::Arc;

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
            let data = vec![0u8; total_bits as usize / 8 + 5];
            (data, total_bits, num_lines)
        } else {
            (vec![0u8; 5], 0, 0)
        }
    }

    fn add_hash(&self, mut h: u32, data: &mut Vec<u8>, num_lines: u32) {
        let delta = (h >> 17) | (h << 15);
        let b = (h % num_lines) * (CACHE_LINE_SIZE * 8);
        for _ in 0..self.num_probes {
            let bitpos = b + (h % (CACHE_LINE_SIZE * 8));
            data[bitpos as usize / 8] |= (1 << (bitpos % 8)) as u8;
            h = h.wrapping_add(delta);
        }
    }

    fn finish(&mut self) -> Vec<u8> {
        let (mut data, total_bits, num_lines) = self.calculate_space(self.hash_entries.len());
        let hash_entries = std::mem::take(&mut self.hash_entries);
        if total_bits != 0 && num_lines != 0 {
            for h in hash_entries {
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
    prefix_extractor: Option<Arc<dyn SliceTransform>>,
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
    pub fn new(opts: &BlockBasedTableOptions, filter_bits_builder: FullFilterBitsBuilder) -> Self {
        FullFilterBlockBuilder {
            prefix_extractor: opts.prefix_extractor.clone(),
            filter_bits_builder,
            whole_key_filtering: opts.whole_key_filtering,
            last_whole_key_recorded: false,
            last_prefix_recorded: false,
            last_whole_key_str: vec![],
            last_prefix_str: vec![],
            filter_data: vec![],
            num_added: 0,
        }
    }

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

    fn finish(&mut self) -> crate::common::Result<&[u8]> {
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

pub struct FullFilterBlockFactory {
    bits_per_key: usize,
    num_probes: usize,
}

impl FullFilterBlockFactory {
    pub fn new(bits_per_key: usize) -> Self {
        let mut num_probes = (bits_per_key as f64 * 0.69).round() as usize; // 0.69 =~ ln(2)
        if num_probes < 1 {
            num_probes = 1;
        }
        if num_probes > 30 {
            num_probes = 30;
        }
        Self {
            bits_per_key,
            num_probes,
        }
    }
}

impl FilterBlockFactory for FullFilterBlockFactory {
    fn create_builder(&self, opts: &BlockBasedTableOptions) -> Box<dyn FilterBlockBuilder> {
        let bits = FullFilterBitsBuilder {
            hash_entries: vec![],
            bits_per_key: self.bits_per_key,
            num_probes: self.num_probes,
        };
        let builder = FullFilterBlockBuilder::new(opts, bits);
        Box::new(builder)
    }

    fn create_filter_reader(&self, filter_block: Vec<u8>) -> Box<dyn FilterBlockReader> {
        Box::new(FullFilterBlockReader::new(filter_block))
    }

    fn name(&self) -> &'static str {
        "rocksdb.BuiltinBloomFilter"
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_builder() {
        let mut options = BlockBasedTableOptions::default();
        options.whole_key_filtering = true;
        let factory = FullFilterBlockFactory::new(10);
        let mut builder = factory.create_builder(&options);
        builder.add(b"abcdeeeeee");
        builder.add(b"abcdefffff");
        builder.add(b"abcdeggggg");
        builder.add(b"abcdehhhhh");
        builder.add(b"abcdeiiiii");
        builder.add(b"abcdejjjjj");
        let data = builder.finish().unwrap().to_vec();
        let reader = factory.create_filter_reader(data);
        assert!(reader.key_may_match(b"abcdeeeeee"));
        assert!(reader.key_may_match(b"abcdefffff"));
        assert!(reader.key_may_match(b"abcdeggggg"));
        assert!(reader.key_may_match(b"abcdehhhhh"));
        assert!(reader.key_may_match(b"abcdeiiiii"));
        assert!(reader.key_may_match(b"abcdejjjjj"));
        assert!(!reader.key_may_match(b"abcdejjjjjk"));
        assert!(!reader.key_may_match(b"abcdejjjj"));
        assert!(!reader.key_may_match(b"abcdejjjjjj"));
    }
}
