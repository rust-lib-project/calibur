use crate::common::format::pack_sequence_and_type;
use crate::common::ValueType;
use crate::memtable::concurrent_arena::ConcurrentArena;
use crate::util::{encode_var_uint32, get_var_uint32, varint_length};
use rand::{thread_rng, RngCore};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const MAX_HEIGHT: usize = 12;
const MAX_POSSIBLE_HEIGHT: usize = 32;
const BRANCHING_FACTOR: usize = 4;
const SCALED_INVERSE_BRANCHING: usize = (2147483647usize + 1) / BRANCHING_FACTOR;

#[repr(C)]
struct Node {
    next: [AtomicPtr<Node>; 1],
}

impl Node {
    unsafe fn key(&self) -> *const u8 {
        (self.next.as_ptr() as *const u8).add(std::mem::size_of::<AtomicPtr<Node>>())
    }

    unsafe fn get_next(&self, level: usize) -> *mut Node {
        (*(self.next.as_ptr().sub(level))).load(Ordering::Acquire)
    }

    unsafe fn set_next(&self, level: usize, x: *mut Node) {
        (*(self.next.as_ptr().sub(level))).store(x, Ordering::Release)
    }

    unsafe fn no_barrier_set_next(&self, level: usize, x: *mut Node) {
        (*(self.next.as_ptr().sub(level))).store(x, Ordering::Relaxed)
    }

    unsafe fn cas_next(&self, level: usize, old: *mut Node, x: *mut Node) -> bool {
        (*(self.next.as_ptr().sub(level)))
            .compare_exchange(old, x, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    unsafe fn insert_after(&mut self, level: usize, prev: *mut Node) {
        self.no_barrier_set_next(level, (*prev).get_next(level));
        (*prev).set_next(level, self)
    }
}

struct Splice {
    height: usize,
    prev: [*mut Node; MAX_POSSIBLE_HEIGHT],
    next: [*mut Node; MAX_POSSIBLE_HEIGHT],
}

impl Default for Splice {
    fn default() -> Self {
        Self {
            height: 0,
            prev: [null_mut(); MAX_POSSIBLE_HEIGHT],
            next: [null_mut(); MAX_POSSIBLE_HEIGHT],
        }
    }
}

pub trait Comparator: Sync {
    unsafe fn compare_raw_key(&self, k1: *const u8, k2: *const u8) -> i32;
    unsafe fn compare_key(&self, k1: *const u8, k2: &[u8]) -> i32;
}

pub struct InlineSkipList<C: Comparator> {
    arena: ConcurrentArena,
    head: *mut Node,
    max_height: AtomicUsize,
    cmp: C,
}

impl<C: Comparator> InlineSkipList<C> {
    pub fn random_height(&self) -> usize {
        let mut height = 1;
        let mut rng = thread_rng();
        while height < MAX_HEIGHT
            && height < MAX_POSSIBLE_HEIGHT
            && (rng.next_u32() as usize) < SCALED_INVERSE_BRANCHING
        {
            height += 1;
        }
        height
    }

    pub fn add<'a>(&self, key: &[u8], value: &[u8], sequence: u64) {
        let mut splice = Splice::default();
        unsafe {
            let (height, node) = self.encode_key_value(key, value, sequence, ValueType::TypeValue);
            splice.height = height;
            self.insert(&mut splice, node);
        }
    }

    pub fn delete(&self, key: &[u8], sequence: u64) {
        let mut splice = Splice::default();
        unsafe {
            let (height, node) = self.encode_key_value(key, &[], sequence, ValueType::TypeDeletion);
            splice.height = height;
            self.insert(&mut splice, node);
        }
    }

    unsafe fn insert(&self, splice: &mut Splice, x: *mut Node) {
        let mut max_height = self.max_height.load(Ordering::Acquire);
        while splice.height > max_height {
            match self.max_height.compare_exchange_weak(
                max_height,
                splice.height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(v) => {
                    max_height = v;
                    break;
                }
                Err(v) => {
                    max_height = v;
                }
            }
        }
        assert!(max_height <= MAX_POSSIBLE_HEIGHT);
        let key = self.decode_key((*x).key());
        splice.next[max_height] = null_mut();
        splice.prev[max_height] = self.head;
        for i in 0..max_height {
            let idx = max_height - i - 1;
            let (prev, next) =
                self.find_splice_for_level(key, splice.prev[idx + 1], splice.next[idx + 1], i);
            splice.prev[i] = prev;
            splice.next[i] = next;
        }
        for i in 0..splice.height {
            loop {
                (*x).no_barrier_set_next(i, splice.next[i]);
                if (*splice.prev[i]).cas_next(i, splice.next[i], x) {
                    break;
                }
                let (prev, next) = self.find_splice_for_level(key, splice.prev[i], null_mut(), i);
                splice.prev[i] = prev;
                splice.next[i] = next;
            }
        }
    }

    unsafe fn decode_key(&self, k: *const u8) -> &[u8] {
        let mut offset = 0;
        let key = std::slice::from_raw_parts(k, 5);
        let l = get_var_uint32(key, &mut offset).unwrap();
        std::slice::from_raw_parts(k.add(offset), l as usize)
    }

    unsafe fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        after: *mut Node,
        level: usize,
    ) -> (*mut Node, *mut Node) {
        loop {
            let next = (*before).get_next(level);
            if std::ptr::eq(next, after) || !self.key_is_after_node(key, before) {
                return (before, next);
            }
            before = next;
        }
    }

    unsafe fn key_is_after_node(&self, key: &[u8], x: *const Node) -> bool {
        !std::ptr::eq(x, null_mut()) && self.cmp.compare_key((*x).key(), key) < 0
    }
    unsafe fn raw_key_is_after_node(&self, key: *const u8, x: *const Node) -> bool {
        !std::ptr::eq(x, null_mut()) && self.cmp.compare_raw_key((*x).key(), key) < 0
    }

    #[inline(always)]
    unsafe fn encode_key_value(
        &self,
        key: &[u8],
        value: &[u8],
        sequence: u64,
        tp: ValueType,
    ) -> (usize, *mut Node) {
        let internal_key_size = key.len() + 8;
        let encoded_len = varint_length(internal_key_size)
            + internal_key_size
            + varint_length(value.len())
            + value.len();
        let h = self.random_height();
        let prefix = std::mem::size_of::<AtomicPtr<Node>>() * (h - 1);
        let addr = self
            .arena
            .allocate(prefix + std::mem::size_of::<Node>() + encoded_len);
        let key_addr = addr.add(prefix + std::mem::size_of::<Node>());
        let data = std::slice::from_raw_parts_mut(key_addr, encoded_len);
        let offset = encode_var_uint32(data, internal_key_size as u32);
        let nxt_offset = offset + key.len();
        data[offset..nxt_offset].copy_from_slice(key);
        data[nxt_offset..(nxt_offset + 8)]
            .copy_from_slice(&pack_sequence_and_type(sequence, tp as u8).to_le_bytes());
        let offset = nxt_offset + 8;
        let offset = encode_var_uint32(&mut data[offset..], value.len() as u32) + offset;
        let nxt_offset = offset + value.len();
        data[offset..nxt_offset].copy_from_slice(value);
        (h, addr.add(prefix) as _)
    }

    unsafe fn find_greater_or_equal(&self, key: *const u8) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire) - 1;
        let key_decoded = self.decode_key(key);
        let mut x = self.head;
        let mut last_bigger = null_mut();
        loop {
            let next = (*x).get_next(level);
            let cmp = if std::ptr::eq(next, null_mut()) || std::ptr::eq(next, last_bigger) {
                1
            } else {
                self.cmp.compare_key((*next).key(), key_decoded)
            };
            if cmp == 0 || (cmp > 0 && level == 0) {
                return next;
            } else if cmp < 0 {
                x = next;
            } else {
                last_bigger = next;
                level -= 1;
            }
        }
    }
}

pub struct SkipListIterator<C: Comparator> {
    list: *const InlineSkipList<C>,
    node: *mut Node,
}

impl<C: Comparator> SkipListIterator<C> {
    fn new(list: *const InlineSkipList<C>) -> Self {
        Self {
            list,
            node: null_mut(),
        }
    }
    pub unsafe fn key(&self) -> *const u8 {
        (*self.node).key()
    }

    pub unsafe fn seek(&mut self, k: *const u8) {
        self.node = (*self.list).find_greater_or_equal(k);
    }

    pub fn valid(&self) -> bool {
        !std::ptr::eq(self.node, null_mut())
    }

    pub unsafe fn next(&mut self) {
        self.node = (*self.node).get_next(0);
    }

    pub unsafe fn prev(&mut self) {
        unimplemented!()
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            self.node = (*(*self.list).head).get_next(0);
        }
    }
}
