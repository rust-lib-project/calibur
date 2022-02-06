use crate::common::format::{GlobalSeqnoAppliedKey, ParsedInternalKey, ValueType};
use crate::common::{KeyComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER, MAX_SEQUENCE_NUMBER};
use crate::table::{AsyncIterator, InternalIterator};
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
    comparator: Arc<dyn KeyComparator>,
    current_user_key_snapshot: u64,
    earliest_snapshot: u64,
    bottommost_level: bool,
}

impl CompactionIter {
    pub fn new(
        iter: Box<dyn InternalIterator>,
        comparator: Arc<dyn KeyComparator>,
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
            comparator,
            current_user_key_snapshot: 0,
            earliest_snapshot,
            bottommost_level,
        }
    }

    pub fn new_with_async(
        iter: Box<dyn AsyncIterator>,
        comparator: Arc<dyn KeyComparator>,
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
            comparator,
            current_user_key_snapshot: 0,
            earliest_snapshot,
            bottommost_level,
        }
    }

    pub async fn seek_to_first(&mut self) {}

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
        false
    }

    pub fn key(&self) -> &[u8] {
        &self.key
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
    fn inner_value(&self) -> &[u8] {
        match &self.inner {
            InnerIterator::Async(iter) => iter.value(),
            InnerIterator::Sync(iter) => iter.value(),
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
            if !self.has_current_user_key
                || !self
                    .comparator
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
                let valid = match &mut self.inner {
                    InnerIterator::Async(iter) => {
                        iter.next().await;
                        iter.valid()
                    }
                    InnerIterator::Sync(iter) => {
                        iter.next();
                        iter.valid()
                    }
                };
                while valid {
                    let next_key = match &self.inner {
                        InnerIterator::Async(iter) => iter.key(),
                        InnerIterator::Sync(iter) => iter.key(),
                    };
                    let parsed_key = ParsedInternalKey::new(next_key);
                    if !parsed_key.valid()
                        || !self
                            .comparator
                            .same_key(ikey.user_key(), parsed_key.user_key())
                    {
                        break;
                    }

                    if parsed_key.sequence <= prev_snapshot {
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
