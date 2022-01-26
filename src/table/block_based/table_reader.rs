use std::sync::Arc;
use crate::common::{options::ReadOptions, RandomAccessFileReader, Result};
use crate::table::{TableReader, TableReaderIterator, TableReaderOptions};
use async_trait::async_trait;

pub struct BlockBasedTableIterator {}

impl TableReaderIterator for BlockBasedTableIterator {
    fn valid(&self) -> bool {
        todo!()
    }

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }
}

pub struct BlockBasedTable {}

#[async_trait]
impl TableReader for BlockBasedTable {
    async fn get(&self, opts: &ReadOptions, key: &[u8], sequence: u64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn new_iterator(&self, opts: &ReadOptions) -> Box<dyn TableReaderIterator> {
        let it = BlockBasedTableIterator {};
        Box::new(it)
    }
}


impl BlockBasedTable {
    pub async fn open(
        opts: &TableReaderOptions,
        file: Arc<dyn RandomAccessFileReader>,
    ) -> Result<Self> {
        Ok(BlockBasedTable{})
    }
}