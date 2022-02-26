use crate::util::{BTreeIter, BtreeComparable, PageIterator};
use crate::version::TableFile;
use std::sync::Arc;

pub trait TableAccessor: Send {
    fn seek(&mut self, key: &[u8]);
    fn seek_for_previous(&mut self, key: &[u8]);
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn next(&mut self);
    fn prev(&mut self);
    fn valid(&self) -> bool;
    fn size(&self) -> usize;
    fn table(&self) -> Arc<TableFile>;
}

pub struct VecTableAccessor {
    tables: Vec<Arc<TableFile>>,
    cursor: usize,
}

impl VecTableAccessor {
    pub fn new(tables: Vec<Arc<TableFile>>) -> Self {
        Self { tables, cursor: 0 }
    }
}

impl TableAccessor for VecTableAccessor {
    fn seek(&mut self, key: &[u8]) {
        self.cursor = match self.tables.binary_search_by(|node| node.largest().cmp(key)) {
            Ok(idx) => idx,
            Err(upper) => upper,
        };
    }

    fn seek_for_previous(&mut self, key: &[u8]) {
        self.cursor = match self
            .tables
            .binary_search_by(|node| node.smallest().cmp(key))
        {
            Ok(idx) => idx,
            Err(upper) => {
                if upper == 0 {
                    self.tables.len()
                } else {
                    upper - 1
                }
            }
        };
    }

    fn seek_to_first(&mut self) {
        self.cursor = 0;
    }

    fn seek_to_last(&mut self) {
        if self.tables.is_empty() {
            self.cursor = 0;
        } else {
            self.cursor = self.tables.len() - 1;
        }
    }

    fn next(&mut self) {
        self.cursor += 1;
    }

    fn prev(&mut self) {
        if self.cursor == 0 {
            self.cursor = self.tables.len()
        } else {
            self.cursor -= 1;
        }
    }

    fn valid(&self) -> bool {
        self.cursor < self.tables.len()
    }

    fn size(&self) -> usize {
        self.tables.len()
    }

    fn table(&self) -> Arc<TableFile> {
        self.tables[self.cursor].clone()
    }
}

pub struct BTreeTableAccessor {
    iter: BTreeIter<Arc<TableFile>>,
}

impl BTreeTableAccessor {
    pub fn new(iter: BTreeIter<Arc<TableFile>>) -> Self {
        Self { iter }
    }
}

impl TableAccessor for BTreeTableAccessor {
    fn seek(&mut self, key: &[u8]) {
        self.iter.seek(key)
    }

    fn seek_for_previous(&mut self, key: &[u8]) {
        self.iter.seek_for_previous(key)
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn size(&self) -> usize {
        self.iter.size()
    }

    fn table(&self) -> Arc<TableFile> {
        self.iter.record().unwrap()
    }
}
