use crate::common::format::extract_user_key;
use crate::iterator::table_accessor::TableAccessor;
use crate::iterator::AsyncIterator;
use crate::table::{TableCache, TableReader};
use crate::util::CachableEntry;
use crate::version::FileMetaData;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TwoLevelIterator<Acessor: TableAccessor> {
    table_accessor: Acessor,
    current: Option<Box<dyn AsyncIterator>>,
    local_cache: HashMap<u64, CachableEntry<Box<dyn TableReader>>>,
    table_cache: Arc<TableCache>,
}

impl<Accessor: TableAccessor> TwoLevelIterator<Accessor> {
    pub fn new(table_accessor: Accessor, table_cache: Arc<TableCache>) -> Self {
        Self {
            table_accessor,
            current: None,
            table_cache,
            local_cache: HashMap::default(),
        }
    }

    async fn fetch_table(&mut self, meta: &FileMetaData) -> Box<dyn AsyncIterator> {
        {
            if let Some(table) = self.local_cache.get(&meta.id()) {
                return table.value().new_iterator();
            }
        }
        let table = self
            .table_cache
            .get_table_reader(meta)
            .await
            .unwrap_or_else(|_| {
                panic!("read because io error");
            });
        let iter = table.value().new_iterator();
        self.local_cache.insert(meta.id(), table);
        iter
    }

    async fn forward_iterator(&mut self) {
        while self.table_accessor.valid() {
            let mut iter = self.fetch_table(&self.table_accessor.table().meta).await;
            iter.seek_to_last().await;
            if iter.valid() {
                self.current = Some(iter);
                return;
            }
        }
        self.current = None;
    }

    async fn backward_iterator(&mut self) {
        while self.table_accessor.valid() {
            let mut iter = self.fetch_table(&self.table_accessor.table().meta).await;
            iter.seek_to_first().await;
            if iter.valid() {
                self.current = Some(iter);
                return;
            }
        }
        self.current = None;
    }
}

#[async_trait::async_trait]
impl<Accessor: TableAccessor> AsyncIterator for TwoLevelIterator<Accessor> {
    fn valid(&self) -> bool {
        self.current.as_ref().map_or(false, |iter| iter.valid())
    }

    async fn seek(&mut self, key: &[u8]) {
        self.table_accessor.seek(extract_user_key(key));
        if self.table_accessor.valid() {
            let mut iter = self.fetch_table(&self.table_accessor.table().meta).await;
            iter.seek(key).await;
            if iter.valid() {
                self.current = Some(iter);
                return;
            }
            self.table_accessor.next();
        }
        self.backward_iterator().await;
    }

    async fn seek_for_prev(&mut self, key: &[u8]) {
        self.table_accessor.seek_for_previous(extract_user_key(key));
        if self.table_accessor.valid() {
            let mut iter = self.fetch_table(&self.table_accessor.table().meta).await;
            iter.seek_for_prev(key).await;
            if iter.valid() {
                self.current = Some(iter);
                return;
            }
            self.table_accessor.prev();
        }
        self.forward_iterator().await;
    }
    async fn seek_to_first(&mut self) {
        self.table_accessor.seek_to_first();
        self.backward_iterator().await;
    }

    async fn seek_to_last(&mut self) {
        self.table_accessor.seek_to_last();
        self.forward_iterator().await;
    }

    async fn next(&mut self) {
        self.current.as_mut().unwrap().next().await;
        if self.current.as_ref().unwrap().valid() {
            return;
        }
        self.table_accessor.next();
        self.backward_iterator().await;
    }

    async fn prev(&mut self) {
        self.current.as_mut().unwrap().prev().await;
        if self.current.as_ref().unwrap().valid() {
            return;
        }
        self.table_accessor.prev();
        self.forward_iterator().await;
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }
}
