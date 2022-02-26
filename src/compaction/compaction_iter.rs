use crate::common::format::{GlobalSeqnoAppliedKey, ParsedInternalKey, ValueType};
use crate::common::{KeyComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER, MAX_SEQUENCE_NUMBER};
use crate::iterator::{AsyncIterator, InternalIterator};
use std::sync::Arc;

enum InnerIterator {
    Async(Box<dyn AsyncIterator>),
    Sync(Box<dyn InternalIterator>),
}

pub struct CompactionIter {
    inner: InnerIterator,
    applied_key: GlobalSeqnoAppliedKey,
    snapshots: Vec<u64>,
    key: Vec<u8>,
    value: Vec<u8>,
    at_next: bool,
    valid: bool,
    has_current_user_key: bool,
    user_comparator: Arc<dyn KeyComparator>,
    current_user_key_snapshot: u64,
    current_sequence: u64,
    earliest_snapshot: u64,
    bottommost_level: bool,
}

impl CompactionIter {
    pub fn new(
        iter: Box<dyn InternalIterator>,
        user_comparator: Arc<dyn KeyComparator>,
        snapshots: Vec<u64>,
        bottommost_level: bool,
    ) -> Self {
        let earliest_snapshot = snapshots.first().cloned().unwrap_or(MAX_SEQUENCE_NUMBER);
        Self {
            inner: InnerIterator::Sync(iter),
            applied_key: GlobalSeqnoAppliedKey::new(DISABLE_GLOBAL_SEQUENCE_NUMBER, false),
            snapshots,
            key: vec![],
            value: vec![],
            at_next: false,
            valid: false,
            has_current_user_key: false,
            user_comparator,
            current_user_key_snapshot: 0,
            current_sequence: 0,
            earliest_snapshot,
            bottommost_level,
        }
    }

    pub fn new_with_async(
        iter: Box<dyn AsyncIterator>,
        user_comparator: Arc<dyn KeyComparator>,
        snapshots: Vec<u64>,
        bottommost_level: bool,
    ) -> Self {
        let earliest_snapshot = snapshots.first().cloned().unwrap_or(MAX_SEQUENCE_NUMBER);
        Self {
            inner: InnerIterator::Async(iter),
            applied_key: GlobalSeqnoAppliedKey::new(0, false),
            snapshots,
            key: vec![],
            value: vec![],
            at_next: false,
            valid: false,
            has_current_user_key: false,
            user_comparator,
            current_user_key_snapshot: 0,
            current_sequence: 0,
            earliest_snapshot,
            bottommost_level,
        }
    }

    pub async fn seek_to_first(&mut self) {
        match &mut self.inner {
            InnerIterator::Async(iter) => iter.seek_to_first().await,
            InnerIterator::Sync(iter) => iter.seek_to_first(),
        }
        self.next_from_input().await;
    }

    pub async fn next(&mut self) {
        if !self.at_next {
            match &mut self.inner {
                InnerIterator::Async(iter) => iter.next().await,
                InnerIterator::Sync(iter) => iter.next(),
            }
        }
        self.next_from_input().await;
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn current_sequence(&self) -> u64 {
        self.current_sequence
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    fn inner_valid(&self) -> bool {
        match &self.inner {
            InnerIterator::Async(iter) => iter.valid(),
            InnerIterator::Sync(iter) => iter.valid(),
        }
    }

    async fn next_from_input(&mut self) {
        self.at_next = false;
        self.valid = false;
        while !self.valid && self.inner_valid() {
            {
                let (key, value) = match &self.inner {
                    InnerIterator::Async(iter) => (iter.key(), iter.value()),
                    InnerIterator::Sync(iter) => (iter.key(), iter.value()),
                };
                self.key.clear();
                self.value.clear();
                self.key.extend_from_slice(key);
                self.value.extend_from_slice(value);
            }
            let ikey = ParsedInternalKey::new(&self.key);
            if !ikey.valid() {
                // TODO: record error
                self.valid = false;
                break;
            }
            self.current_sequence = ikey.sequence;
            if !self.has_current_user_key
                || !self
                    .user_comparator
                    .same_key(ikey.user_key(), self.applied_key.get_user_key())
            {
                self.applied_key.set_key(&self.key);
                self.has_current_user_key = true;
                self.current_user_key_snapshot = 0;
            } else {
                self.applied_key.update_internal_key(ikey.sequence, ikey.tp);
            }

            let last_snapshot = self.current_user_key_snapshot;
            let (prev_snapshot, after_snapshot) =
                self.find_earliest_visible_snapshot(ikey.sequence);
            self.current_user_key_snapshot = after_snapshot;

            assert!(self.current_user_key_snapshot > 0);
            // It means that this key is same to the last key and the last snapshot can not visit
            // this key, so we can remove it safely.
            // TODO: update snapshot list after compaction running a long time.
            if last_snapshot == self.current_user_key_snapshot {
                match &mut self.inner {
                    InnerIterator::Async(iter) => iter.next().await,
                    InnerIterator::Sync(iter) => iter.next(),
                }
            }
            /* else if ikey.tp == ValueType::TypeDeletion && ikey.sequence < self.earliest_snapshot
             && compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key(),
                                                          &level_ptrs_){
                TODO: if this key will not appear in bottom level, we can delete it in advance.
                match &mut self.inner {
                    InnerIterator::Async(iter) => {
                        iter.next().await;
                    },
                    InnerIterator::Sync(iter) => {
                        iter.next();
                    },
                };
            } */
            else if ikey.tp == ValueType::TypeDeletion && self.bottommost_level {
                while inner_next(&mut self.inner).await {
                    let next_key = match &self.inner {
                        InnerIterator::Async(iter) => iter.key(),
                        InnerIterator::Sync(iter) => iter.key(),
                    };
                    let parsed_key = ParsedInternalKey::new(next_key);
                    if !parsed_key.valid()
                        || !self
                            .user_comparator
                            .same_key(ikey.user_key(), parsed_key.user_key())
                    {
                        break;
                    }

                    if prev_snapshot > 0 && parsed_key.sequence <= prev_snapshot {
                        self.valid = true;
                        self.at_next = true;
                        break;
                    }
                }
            } else {
                self.valid = true;
            }
        }
    }

    fn find_earliest_visible_snapshot(&self, current: u64) -> (u64, u64) {
        if self.earliest_snapshot == MAX_SEQUENCE_NUMBER {
            return (0, MAX_SEQUENCE_NUMBER);
        }
        let pos = match self.snapshots.binary_search(&current) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let largest = if pos < self.snapshots.len() {
            self.snapshots[pos]
        } else {
            MAX_SEQUENCE_NUMBER
        };
        if pos > 0 {
            (self.snapshots[pos - 1], largest)
        } else {
            (0, largest)
        }
    }
}

async fn inner_next(inner: &mut InnerIterator) -> bool {
    match inner {
        InnerIterator::Async(iter) => {
            iter.next().await;
            iter.valid()
        }
        InnerIterator::Sync(iter) => {
            iter.next();
            iter.valid()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::format::pack_sequence_and_type;
    use crate::common::InternalKeyComparator;
    use crate::memtable::Memtable;
    use tokio::runtime::Runtime;

    fn test_compaction_with_snapshot(
        memtable: &Memtable,
        comparator: &InternalKeyComparator,
        snapshots: Vec<u64>,
        expected_ret: Vec<(Vec<u8>, Vec<u8>)>,
        bottommost_level: bool,
    ) {
        let iter = memtable.new_iterator();
        let mut compaction_iter = CompactionIter::new(
            iter,
            comparator.get_user_comparator().clone(),
            snapshots,
            bottommost_level,
        );
        let f = async move {
            let mut data = vec![];
            compaction_iter.seek_to_first().await;
            while compaction_iter.valid() {
                let key = compaction_iter.key().to_vec();
                let value = compaction_iter.value().to_vec();
                data.push((key, value));
                compaction_iter.next().await;
            }
            data
        };
        let r = Runtime::new().unwrap();
        let ret = r.block_on(f);
        assert_eq!(ret.len(), expected_ret.len());
        for i in 0..ret.len() {
            assert_eq!(ret[i], expected_ret[i]);
        }
    }

    #[test]
    fn test_compaction_iterator() {
        let comparator = InternalKeyComparator::default();
        let memtable = Memtable::new(10, 10, comparator.clone(), MAX_SEQUENCE_NUMBER);

        // no pending snapshot
        let mut expected_ret1 = vec![];

        // a pending snapshot with 12
        let mut expected_ret2 = vec![];

        // a pending snapshot with 14
        let mut expected_ret3 = vec![];

        // a pending snapshot with 10, 14
        let mut expected_ret4 = vec![];

        // no pending snapshot with 14 and bottommost level
        let mut expected_ret5 = vec![];

        for i in 0..1000u64 {
            let key = format!("test_compaction-{}", i);
            let mut k1 = key.into_bytes();
            let l = k1.len();

            let v0 = pack_sequence_and_type(10, ValueType::TypeValue as u8);
            k1.extend_from_slice(&v0.to_le_bytes());
            memtable.insert(&k1, b"v0");
            expected_ret4.push((k1.clone(), b"v0".to_vec()));
            k1.resize(l, 0);

            let v1 = pack_sequence_and_type(12, ValueType::TypeValue as u8);
            k1.extend_from_slice(&v1.to_le_bytes());
            memtable.insert(&k1, b"v1");
            if i % 2 != 0 {
                expected_ret1.push((k1.clone(), b"v1".to_vec()));
                expected_ret3.push((k1.clone(), b"v1".to_vec()));
                expected_ret4.push((k1.clone(), b"v1".to_vec()));
                expected_ret5.push((k1.clone(), b"v1".to_vec()));
            }
            expected_ret2.push((k1.clone(), b"v1".to_vec()));

            k1.resize(l, 0);

            if i % 2 == 0 {
                let v2 = pack_sequence_and_type(14, ValueType::TypeDeletion as u8);
                k1.extend_from_slice(&v2.to_le_bytes());
                memtable.insert(&k1, b"");
                if i % 4 != 0 {
                    expected_ret1.push((k1.clone(), vec![]));
                    expected_ret2.push((k1.clone(), vec![]));
                }
                expected_ret3.push((k1.clone(), vec![]));
                expected_ret4.push((k1.clone(), vec![]));
                k1.resize(l, 0);
            }
            if i % 4 == 0 {
                let v3 = pack_sequence_and_type(16, ValueType::TypeValue as u8);
                k1.extend_from_slice(&v3.to_le_bytes());
                memtable.insert(&k1, b"v3");
                expected_ret1.push((k1.clone(), b"v3".to_vec()));
                expected_ret2.push((k1.clone(), b"v3".to_vec()));
                expected_ret3.push((k1.clone(), b"v3".to_vec()));
                expected_ret4.push((k1.clone(), b"v3".to_vec()));
                expected_ret5.push((k1, b"v3".to_vec()));
            }
        }
        expected_ret1.sort_by(|a, b| comparator.compare_key(&a.0, &b.0));
        expected_ret2.sort_by(|a, b| comparator.compare_key(&a.0, &b.0));
        expected_ret3.sort_by(|a, b| comparator.compare_key(&a.0, &b.0));
        expected_ret4.sort_by(|a, b| comparator.compare_key(&a.0, &b.0));
        expected_ret5.sort_by(|a, b| comparator.compare_key(&a.0, &b.0));
        test_compaction_with_snapshot(&memtable, &comparator, vec![], expected_ret1, false);
        test_compaction_with_snapshot(&memtable, &comparator, vec![12], expected_ret2, false);
        test_compaction_with_snapshot(&memtable, &comparator, vec![14], expected_ret3, false);
        test_compaction_with_snapshot(&memtable, &comparator, vec![10, 14], expected_ret4, false);
        test_compaction_with_snapshot(&memtable, &comparator, vec![14], expected_ret5, true);
    }
}
