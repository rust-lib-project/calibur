use std::sync::Arc;
use crate::common::InternalKeyComparator;
use crate::table::block_based::block::IndexBlockIter;
use crate::table::InternalIterator;

pub struct BlockBasedTableIterator {
    comparator: Arc<InternalKeyComparator>,
    index_iter: Box<dyn InternalIterator>,
}

impl BlockBasedTableIterator {
    pub fn new(
        comparator: Arc<InternalKeyComparator>,
        index_iter: Box<dyn InternalIterator>,
    ) -> Self {
        Self {
            comparator,
            index_iter,
        }
    }
}

impl InternalIterator for BlockBasedTableIterator {
    fn valid(&self) -> bool {
        todo!()
    }

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
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
