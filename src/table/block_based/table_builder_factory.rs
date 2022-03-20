use crate::common::{RandomAccessFile, WritableFileWriter};
use crate::table::block_based::options::BlockBasedTableOptions;
use crate::table::block_based::table_builder::BlockBasedTableBuilder;
use crate::table::block_based::table_reader::BlockBasedTable;
use crate::table::{
    TableBuilder, TableBuilderOptions, TableFactory, TableReader, TableReaderOptions,
};
use async_trait::async_trait;

#[derive(Default)]
pub struct BlockBasedTableFactory {
    opts: BlockBasedTableOptions,
}

impl BlockBasedTableFactory {
    pub fn new(opts: BlockBasedTableOptions) -> Self {
        Self { opts }
    }
}

#[async_trait]
impl TableFactory for BlockBasedTableFactory {
    fn name(&self) -> &'static str {
        "BlockBasedTableFactory"
    }

    async fn open_reader(
        &self,
        opts: &TableReaderOptions,
        file: Box<dyn RandomAccessFile>,
    ) -> crate::common::Result<Box<dyn TableReader>> {
        let reader = BlockBasedTable::open(opts, self.opts.clone(), file).await?;
        Ok(Box::new(reader))
    }

    fn new_builder(
        &self,
        opts: &TableBuilderOptions,
        file: Box<WritableFileWriter>,
    ) -> crate::common::Result<Box<dyn TableBuilder>> {
        let builder = BlockBasedTableBuilder::new(opts, self.opts.clone(), file);
        Ok(Box::new(builder))
    }
}
