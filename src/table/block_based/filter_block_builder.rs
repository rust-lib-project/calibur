use crate::common::Result;
use crate::table::block_based::block::Block;
use crate::table::block_based::filter_reader::FilterBlockReader;
use crate::table::block_based::options::BlockBasedTableOptions;

pub trait FilterBlockBuilder {
    fn is_block_based(&self) -> bool {
        false
    }
    fn add(&mut self, key: &[u8]);
    fn start_block(&mut self, offset: u64);
    fn finish(&mut self) -> Result<&[u8]>;
    fn num_added(&self) -> usize;
}

pub trait FilterBlockFactory: Send + Sync {
    fn create_builder(&self, opts: &BlockBasedTableOptions) -> Box<dyn FilterBlockBuilder>;
    fn create_filter_reader(&self, filter_block: Vec<u8>) -> Box<dyn FilterBlockReader>;
    fn name(&self) -> &'static str;
    fn is_block_based(&self) -> bool {
        false
    }
}
