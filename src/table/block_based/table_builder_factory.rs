use crate::common::{FixedLengthSuffixComparator, RandomAccessFileReader, WritableFileWriter};
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::table::block_based::table_builder::BlockBasedTableBuilder;
use crate::table::{TableBuilder, TableFactory, TableReader};
use std::sync::Arc;

pub struct BlockBasedTableFactory {
    opts: BlockBasedTableOptions,
}

impl TableFactory for BlockBasedTableFactory {
    fn name(&self) -> &'static str {
        "BlockBasedTableFactory"
    }

    fn new_reader(
        &self,
        file: Arc<dyn RandomAccessFileReader>,
    ) -> crate::common::Result<Arc<dyn TableReader>> {
        todo!()
    }

    fn new_builder(
        &self,
        skip_filters: bool,
        file: WritableFileWriter,
    ) -> crate::common::Result<Box<dyn TableBuilder>> {
        let builder = BlockBasedTableBuilder::new(
            self.opts.clone(),
            FixedLengthSuffixComparator::new(8),
            skip_filters,
            file,
        );
        Ok(Box::new(builder))
    }
}
