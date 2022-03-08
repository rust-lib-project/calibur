use crate::common::format::{
    pack_sequence_and_type, ParsedInternalKey, ValueType, VALUE_TYPE_FOR_SEEK,
    VALUE_TYPE_FOR_SEEK_FOR_PREV,
};
use crate::iterator::AsyncIterator;
use crate::KeyComparator;
use std::sync::Arc;

pub struct DBIterator {
    user_comparator: Arc<dyn KeyComparator>,
    inner: Box<dyn AsyncIterator>,
    sequence: u64,
    buf: Vec<u8>,
    current_user_key: Vec<u8>,
    last_key_type: ValueType,
    is_backward: bool,
    pinned_value: Vec<u8>,
}

impl DBIterator {
    pub fn new(
        inner: Box<dyn AsyncIterator>,
        user_comparator: Arc<dyn KeyComparator>,
        sequence: u64,
    ) -> DBIterator {
        DBIterator {
            inner,
            user_comparator,
            sequence,
            last_key_type: ValueType::TypeValue,
            buf: vec![],
            current_user_key: vec![],
            is_backward: false,
            pinned_value: vec![],
        }
    }

    pub fn valid(&self) -> bool {
        if self.is_backward {
            self.inner.valid()
        } else {
            !self.current_user_key.is_empty()
        }
    }

    pub async fn seek(&mut self, key: &[u8]) {
        self.buf.clear();
        self.buf.extend_from_slice(key);
        let num = pack_sequence_and_type(self.sequence, VALUE_TYPE_FOR_SEEK);
        self.buf.extend_from_slice(&num.to_le_bytes());
        self.inner.seek(&self.buf).await;
        self.backward_for_user_key().await;
        self.is_backward = true;
    }

    pub async fn seek_for_prev(&mut self, key: &[u8]) {
        self.buf.clear();
        self.buf.extend_from_slice(key);
        let num = pack_sequence_and_type(self.sequence, VALUE_TYPE_FOR_SEEK_FOR_PREV);
        self.buf.extend_from_slice(&num.to_le_bytes());
        self.inner.seek_for_prev(&self.buf).await;
        self.is_backward = false;
        self.current_user_key.clear();
        self.pinned_value.clear();
        if !self.inner.valid() {
            return;
        }
        let ikey = ParsedInternalKey::new(self.inner.key());
        if ikey.tp == ValueType::TypeValue && ikey.sequence <= self.sequence {
            self.current_user_key.extend_from_slice(ikey.user_key());
            self.pinned_value.extend_from_slice(self.inner.value());
        }
        self.forward_for_user_key().await;
    }

    pub async fn seek_to_first(&mut self) {
        self.is_backward = true;
        self.inner.seek_to_first().await;
        self.backward_for_user_key().await;
    }

    pub async fn seek_to_last(&mut self) {
        self.inner.seek_to_last().await;
        self.current_user_key.clear();
        self.pinned_value.clear();
        self.is_backward = false;
        if !self.inner.valid() {
            return;
        }
        let ikey = ParsedInternalKey::new(self.inner.key());
        if ikey.tp == ValueType::TypeValue && ikey.sequence <= self.sequence {
            self.current_user_key.extend_from_slice(ikey.user_key());
            self.pinned_value.extend_from_slice(self.inner.value());
        }
        self.forward_for_user_key().await;
    }

    pub async fn next(&mut self) {
        self.inner.next().await;
        self.backward_for_user_key().await;
    }

    pub async fn prev(&mut self) {
        self.current_user_key.clear();
        self.pinned_value.clear();
        let ikey = ParsedInternalKey::new(self.inner.key());
        if ikey.tp == ValueType::TypeValue {
            self.current_user_key.extend_from_slice(ikey.user_key());
            self.pinned_value.extend_from_slice(self.inner.value());
        }
        self.inner.prev().await;
        self.forward_for_user_key().await;
    }

    pub fn key(&self) -> &[u8] {
        &self.current_user_key
    }

    pub fn value(&self) -> &[u8] {
        if self.is_backward {
            self.inner.value()
        } else {
            &self.pinned_value
        }
    }
}

impl DBIterator {
    async fn backward_for_user_key(&mut self) {
        self.current_user_key.clear();
        while self.inner.valid() {
            let key = self.inner.key();
            let ikey = ParsedInternalKey::new(key);
            if ikey.sequence > self.sequence {
                self.current_user_key.clear();
            } else if ikey.tp == ValueType::TypeValue {
                if self.current_user_key.is_empty()
                    || !self
                        .user_comparator
                        .same_key(&self.current_user_key, ikey.user_key())
                {
                    self.current_user_key.extend_from_slice(ikey.user_key());
                    return;
                }
            } else if ikey.tp == ValueType::TypeDeletion
                && (self.current_user_key.is_empty()
                    || !self
                        .user_comparator
                        .same_key(&self.current_user_key, ikey.user_key()))
            {
                self.current_user_key.clear();
                self.current_user_key.extend_from_slice(ikey.user_key());
            }
            self.inner.next().await;
        }
    }

    async fn forward_for_user_key(&mut self) {
        while self.inner.valid() {
            let key = self.inner.key();
            let ikey = ParsedInternalKey::new(key);
            if ikey.sequence > self.sequence {
                if !self.current_user_key.is_empty() {
                    break;
                }
            } else if ikey.tp == ValueType::TypeValue {
                if self.current_user_key.is_empty() {
                    self.current_user_key.extend_from_slice(ikey.user_key());
                    self.pinned_value.extend_from_slice(self.inner.value());
                } else if !self
                    .user_comparator
                    .same_key(&self.current_user_key, ikey.user_key())
                {
                    break;
                } else {
                    self.current_user_key.clear();
                    self.current_user_key.extend_from_slice(ikey.user_key());
                    self.pinned_value.extend_from_slice(self.inner.value());
                }
            } else if ikey.tp == ValueType::TypeDeletion {
                if !self.current_user_key.is_empty()
                    && !self
                        .user_comparator
                        .same_key(&self.current_user_key, ikey.user_key())
                {
                    break;
                } else {
                    self.current_user_key.clear();
                    self.pinned_value.clear();
                }
            }
            self.inner.prev().await;
        }
    }
}
