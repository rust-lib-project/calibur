mod block_based;

use crate::common::Result;

pub trait SortedStringTable {
    fn get(&self, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>>;
}
