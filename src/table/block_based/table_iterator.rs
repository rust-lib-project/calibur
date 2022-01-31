use crate::common::InternalKeyComparator;
use crate::table::block_based::block::{DataBlockIter, IndexBlockIter};
use crate::table::block_based::table_reader::{BlockBasedTable, BlockBasedTableRep};
use crate::table::{AsyncIterator, InternalIterator};
use async_trait::async_trait;
use std::sync::Arc;

pub struct BlockBasedTableIterator {
    table: Arc<BlockBasedTableRep>,
    comparator: Arc<InternalKeyComparator>,
    index_iter: Box<IndexBlockIter>,
    data_iter: Option<DataBlockIter>,
    is_out_of_bound: bool,
}

impl BlockBasedTableIterator {
    pub fn new(
        table: Arc<BlockBasedTableRep>,
        comparator: Arc<InternalKeyComparator>,
        index_iter: Box<IndexBlockIter>,
    ) -> Self {
        Self {
            table,
            comparator,
            index_iter,
            data_iter: None,
            is_out_of_bound: false,
        }
    }
}

impl BlockBasedTableIterator {
    async fn init_data_block(&mut self) -> bool {
        let v = self.index_iter.index_value();
        match self.table.new_data_block_iterator(&v.handle).await {
            Ok(iter) => {
                self.data_iter = Some(iter);
                true
            }
            Err(_) => {
                // TODO: record the IO Error
                self.data_iter.take();
                false
            }
        }
    }
    async fn find_block_forward(&mut self) {
        while self.data_iter.as_ref().map_or(false, |iter| !iter.valid()) {
            self.data_iter.take();
            self.index_iter.next();
            if !self.index_iter.valid() {
                return;
            }
            if !self.init_data_block().await {
                return;
            }
        }
    }
}

#[async_trait]
impl AsyncIterator for BlockBasedTableIterator {
    fn valid(&self) -> bool {
        self.data_iter.as_ref().map_or(false, |iter| iter.valid())
    }

    async fn seek(&mut self, key: &[u8]) {
        // TODO: check prefix seek
        // TODO: near seek, do not seek index iterator.
        if key.is_empty() {
            self.index_iter.seek_to_first();
        } else {
            self.index_iter.seek(key);
        }
        if !self.index_iter.valid() {
            self.data_iter.take();
            return;
        }
        if !self.init_data_block().await {
            return;
        }
        // TODO: check whether the data block is in upper bound.
        assert!(self.data_iter.is_some());
        if key.is_empty() {
            self.data_iter.as_mut().unwrap().seek_to_first();
        } else {
            self.data_iter.as_mut().unwrap().seek(key);
        }
        if !self.data_iter.as_ref().unwrap().valid() {
            self.find_block_forward().await;
        }
    }

    async fn seek_to_first(&mut self) {
        self.seek(&[]).await;
    }

    async fn seek_to_last(&mut self) {
        todo!()
    }

    async fn seek_for_prev(&mut self, key: &[u8]) {
        todo!()
    }

    async fn next(&mut self) {
        todo!()
    }

    async fn prev(&mut self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }
}
