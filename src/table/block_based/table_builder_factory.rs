use crate::common::{RandomAccessFileReader, WritableFileWriter};
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::table::block_based::table_builder::BlockBasedTableBuilder;
use crate::table::{TableBuilder, TableBuilderOptions, TableFactory, TableReader};
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
        opts: &TableBuilderOptions,
        file: WritableFileWriter,
    ) -> crate::common::Result<Box<dyn TableBuilder>> {
        let builder = BlockBasedTableBuilder::new(opts, self.opts.clone(), file);
        Ok(Box::new(builder))
    }
}
