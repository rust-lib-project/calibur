use crate::common::key_hash;

const kMaxRestartSupportedByHashIndex: usize = 253;

pub struct DataBlockHashIndexBuilder {
    valid: bool,
    bucket_per_key: f64,
    estimated_num_buckets: f64,
    hash_and_restart_pairs: Vec<(u32, u8)>,
}

impl DataBlockHashIndexBuilder {
    pub fn init(&mut self, mut ratio: f64) {
        if ratio <= 0.0 {
           ratio = 0.75;
        }
        self.bucket_per_key = 1 / ratio;
        self.valid = true;
    }

    pub fn clear(&mut self) {
        self.estimated_num_buckets = 0.0;
        self.valid = true;
        self.hash_and_restart_pairs.clear();
    }

    pub fn valid(&self) -> bool {
        self.valid && self.bucket_per_key > 0.0
    }

    pub fn add(&mut self, user_key: &[u8], restart_index: usize) {
        if restart_index > kMaxRestartSupportedByHashIndex {
            self.valid = false;
            return;
        }
        let h = key_hash(user_key);
        self.hash_and_restart_pairs.push((h, restart_index as u8));
        self.estimated_num_buckets += self.bucket_per_key;
    }

    pub fn finish(&mut self, data: &mut Vec<u8>) {
        let mut num_buckets = self.estimated_num_buckets.round() as u16;
        if num_buckets == 0 {
            num_buckets = 1;
        }
    }

    pub fn estimate_size(&self) -> usize {
        let mut estimated_num_buckets =
            self.estimated_num_buckets.round() as u16;

        // Maching the num_buckets number in DataBlockHashIndexBuilder::Finish.
        estimated_num_buckets |= 1;

        std::mem::size_of::<u16>() +
            estimated_num_buckets as usize * std::mem::size_of()
    }
}