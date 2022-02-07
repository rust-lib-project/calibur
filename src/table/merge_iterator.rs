use crate::common::{InternalKeyComparator, KeyComparator};
use crate::table::InternalIterator;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::rc::Rc;

pub struct IteratorWrapper {
    inner: Box<dyn InternalIterator>,
    comparator: Rc<InternalKeyComparator>,
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
    pub fn new(iters: Vec<Box<dyn InternalIterator>>, cmp: InternalKeyComparator) -> Self {
        let comparator = Rc::new(cmp);
        let other: Vec<IteratorWrapper> = iters
            .into_iter()
            .map(|iter| IteratorWrapper {
                inner: iter,
                comparator: comparator.clone(),
            })
            .collect();
        Self {
            children: BinaryHeap::with_capacity(other.len()),
            other,
        }
    }
}

impl MergingIterator {
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

impl InternalIterator for MergingIterator {
    fn valid(&self) -> bool {
        self.children
            .peek()
            .map_or(false, |iter| iter.inner.valid())
    }

    fn seek(&mut self, key: &[u8]) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek(key);
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    fn seek_to_first(&mut self) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek_to_first();
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    fn seek_to_last(&mut self) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek_to_last();
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        let iters = self.collect_iterators();
        for mut iter in iters {
            iter.inner.seek_for_prev(key);
            if iter.inner.valid() {
                self.children.push(iter);
            } else {
                self.other.push(iter);
            }
        }
    }

    fn next(&mut self) {
        {
            let mut x = self.children.peek_mut().unwrap();
            x.inner.next();
        }
        self.current_forward();
    }

    fn prev(&mut self) {
        let mut x = self.children.peek_mut().unwrap();
        x.inner.prev();
    }

    fn key(&self) -> &[u8] {
        self.children.peek().unwrap().inner.key()
    }

    fn value(&self) -> &[u8] {
        self.children.peek().unwrap().inner.value()
    }
}
