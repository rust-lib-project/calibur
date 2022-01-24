use crate::common::format::BlockHandle;
use crate::common::Result;

pub trait FilterBlockBuilder {
    fn is_block_based(&self) -> bool {
        false
    }
    fn add(&mut self, key: &[u8]);
    fn start_block(&mut self, offset: u64);
    fn finish(&mut self, handle: &BlockHandle) -> Result<&[u8]>;
    fn num_added(&self) -> usize;
}

pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
    fn create_filter(&self, key: &[u8], n: u32, dst: &mut Vec<u8>);
}

pub trait FilterBuilderFactory {
    fn create_builder(&self) -> Box<dyn FilterBlockBuilder>;
    fn create_policy(&self) -> Box<dyn FilterPolicy>;
}
