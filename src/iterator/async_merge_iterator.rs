use crate::common::{InternalKeyComparator, KeyComparator};
use crate::iterator::AsyncIterator;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct IteratorWrapper {
    inner: Box<dyn AsyncIterator + 'static>,
    comparator: InternalKeyComparator,
}

impl PartialEq<Self> for IteratorWrapper {
    fn eq(&self, other: &Self) -> bool {
        if self.inner.valid() && other.inner.valid() {
            return self
                .comparator
                .same_key(self.inner.key(), other.inner.key());
        }
        if !self.inner.valid() && !other.inner.valid() {
            return true;
        }
        false
    }
}

impl Eq for IteratorWrapper {}

impl PartialOrd<Self> for IteratorWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.inner.valid() && other.inner.valid() {
            self.comparator
                .compare_key(other.inner.key(), self.inner.key())
        } else if self.inner.valid() {
            Ordering::Less
        } else if other.inner.valid() {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

pub struct MergingIterator {
    children: BinaryHeap<IteratorWrapper>,
    other: Vec<IteratorWrapper>,
}

impl MergingIterator {
    pub fn new(iters: Vec<Box<dyn AsyncIterator + 'static>>, cmp: InternalKeyComparator) -> Self {
        let other: Vec<IteratorWrapper> = iters
            .into_iter()
            .map(|iter| IteratorWrapper {
                inner: iter,
                comparator: cmp.clone(),
            })
            .collect();
        Self {
            children: BinaryHeap::with_capacity(other.len()),
            other,
        }
    }

    fn current_forward(&mut self) {
        while let Some(x) = self.children.peek() {
            if !x.inner.valid() {
                let iter = self.children.pop().unwrap();
                self.other.push(iter);
            } else {
                break;
            }
        }
    }

    fn collect_iterators(&mut self) -> Vec<IteratorWrapper> {
        let mut iters = Vec::with_capacity(self.other.len() + self.children.len());
        std::mem::swap(&mut iters, &mut self.other);
        while let Some(iter) = self.children.pop() {
            iters.push(iter);
        }
        iters
    }
}

#[async_trait::async_trait]
impl AsyncIterator for MergingIterator {
    fn valid(&self) -> bool {
        self.children
            .peek()
            .map_or(false, |iter| iter.inner.valid())
    }

    async fn seek(&mut self, key: &[u8]) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek(key).await;
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    async fn seek_to_first(&mut self) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek_to_first().await;
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    async fn seek_to_last(&mut self) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek_to_last().await;
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    async fn seek_for_prev(&mut self, key: &[u8]) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek_for_prev(key).await;
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    async fn next(&mut self) {
        {
            let mut x = self.children.peek_mut().unwrap();
            x.inner.next().await;
        }
        self.current_forward();
    }

    async fn prev(&mut self) {
        {
            let mut x = self.children.peek_mut().unwrap();
            x.inner.prev().await;
        }
        self.current_forward();
    }

    fn key(&self) -> &[u8] {
        self.children.peek().unwrap().inner.key()
    }

    fn value(&self) -> &[u8] {
        self.children.peek().unwrap().inner.value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::extract_user_key;
    use crate::table::InMemTableIterator;
    use tokio::runtime::Runtime;

    #[test]
    fn test_merge_iterator() {
        let mut tables = vec![];
        let mut data = vec![];
        let v = b"v00000000000001";
        let mut ikey = vec![];
        let comparator = InternalKeyComparator::default();
        let mut keys = vec![];
        for i in 0..100 {
            for j in 0..100 {
                let k = (i * 100 + j).to_string();
                ikey.clear();
                ikey.extend_from_slice(k.as_bytes());
                ikey.extend_from_slice(&(i as u64).to_le_bytes());
                data.push((ikey.clone(), v.to_vec()));
                if data.len() > 1600 {
                    let table: Box<dyn AsyncIterator + 'static> =
                        Box::new(InMemTableIterator::new(data.clone(), &comparator));
                    tables.push(table);
                    data.clear();
                }
                keys.push(k);
            }
        }
        if !data.is_empty() {
            let table: Box<dyn AsyncIterator + 'static> =
                Box::new(InMemTableIterator::new(data, &comparator));
            tables.push(table);
        }
        let mut iter = MergingIterator::new(tables, comparator);
        let r = Runtime::new().unwrap();
        r.block_on(iter.seek_to_first());
        let mut i = 0;
        keys.sort();
        while iter.valid() {
            let k = iter.key();
            if let Ok(user_key) = String::from_utf8(extract_user_key(k).to_vec()) {
                assert_eq!(user_key, keys[i]);
            }
            r.block_on(iter.next());
            i += 1;
        }
    }
}
