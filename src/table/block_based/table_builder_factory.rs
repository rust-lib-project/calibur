use crate::common::{RandomAccessFileReader, WritableFileWriter};
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::table::block_based::table_builder::BlockBasedTableBuilder;
use crate::table::block_based::table_reader::BlockBasedTable;
use crate::table::{
    TableBuilder, TableBuilderOptions, TableFactory, TableReader, TableReaderOptions,
};
use async_trait::async_trait;
use std::sync::Arc;

pub struct BlockBasedTableFactory {
    opts: BlockBasedTableOptions,
}

#[async_trait]
impl TableFactory for BlockBasedTableFactory {
    fn name(&self) -> &'static str {
        "BlockBasedTableFactory"
    }

    async fn open_reader(
        &self,
        opts: &TableReaderOptions,
        reader: Box<RandomAccessFileReader>,
    ) -> crate::common::Result<Arc<dyn TableReader>> {
        let reader = BlockBasedTable::open(opts, self.opts.clone(), reader).await?;
        Ok(Arc::new(reader))
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
