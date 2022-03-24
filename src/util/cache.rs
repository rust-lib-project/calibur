use crate::util::hash::key_hash;
use spin::Mutex;
use std::ptr::null_mut;
use std::sync::Arc;

const IN_CACHE: u8 = 1;
const REVERSE_IN_CACHE: u8 = IN_CACHE.swap_bytes();

pub struct LRUHandle<T> {
    next_hash: *mut LRUHandle<T>,
    next: *mut LRUHandle<T>,
    prev: *mut LRUHandle<T>,
    key: u64,
    value: Option<T>,
    charge: usize,
    refs: u32,
    flags: u8,
}

impl<T> LRUHandle<T> {
    pub fn new(key: u64, value: Option<T>) -> Self {
        Self {
            key,
            value,
            next_hash: null_mut(),
            prev: null_mut(),
            next: null_mut(),
            charge: 0,
            flags: 0,
            refs: 0,
        }
    }

    fn set_in_cache(&mut self, in_cache: bool) {
        if in_cache {
            self.flags |= IN_CACHE;
        } else {
            self.flags &= REVERSE_IN_CACHE;
        }
    }

    fn add_ref(&mut self) {
        self.refs += 1;
    }

    fn unref(&mut self) -> bool {
        self.refs -= 1;
        self.refs == 0
    }

    fn has_refs(&self) -> bool {
        self.refs > 0
    }

    fn is_in_cache(&self) -> bool {
        (self.flags & IN_CACHE) > 0
    }
}

unsafe impl<T> Send for LRUHandle<T> {}

pub struct LRUHandleTable<T> {
    list: Vec<*mut LRUHandle<T>>,
    elems: usize,
}

impl<T> LRUHandleTable<T> {
    fn new() -> Self {
        Self {
            list: vec![null_mut(); 16],
            elems: 0,
        }
    }

    unsafe fn find_pointer(&self, key: u64) -> *mut LRUHandle<T> {
        let idx = (key as usize) & (self.list.len() - 1);
        let mut ptr = self.list[idx];
        let mut prev = null_mut();
        while !ptr.is_null() && (*ptr).key != key {
            prev = ptr;
            ptr = (*ptr).next_hash;
        }
        prev
    }

    unsafe fn remove(&mut self, key: u64) -> *mut LRUHandle<T> {
        let mut prev = self.find_pointer(key);
        let mut ret = null_mut();
        if prev.is_null() {
            let idx = (key as usize) & (self.list.len() - 1);
            if !self.list[idx].is_null() && (*self.list[idx]).key == key {
                ret = self.list[idx];
                self.list[idx] = (*self.list[idx]).next_hash;
                self.elems -= 1;
            }
        } else {
            let next = (*prev).next_hash;
            if !next.is_null() && (*next).key == key {
                ret = next;
                (*prev).next_hash = (*next).next_hash;
                self.elems -= 1;
            }
        }
        ret
    }

    unsafe fn insert(&mut self, h: *mut LRUHandle<T>) -> *mut LRUHandle<T> {
        let key = (*h).key;
        let mut prev = self.find_pointer(key);
        if prev.is_null() {
            let idx = (key as usize) & (self.list.len() - 1);
            self.list[idx] = h;
        } else {
            let next = (*prev).next_hash;
            (*prev).next_hash = h;
            if !next.is_null() && (*next).key == key {
                (*h).next_hash = (*next).next_hash;
                return next;
            } else {
                (*h).next_hash = next;
            }
        }
        self.elems += 1;
        if self.elems > self.list.len() {
            self.resize();
        }
        null_mut()
    }

    unsafe fn lookup(&self, key: u64) -> *mut LRUHandle<T> {
        let e = self.find_pointer(key);
        if e.is_null() {
            let idx = (key as usize) & (self.list.len() - 1);
            self.list[idx]
        } else if (*e).next.is_null() {
            null_mut()
        } else {
            (*e).next
        }
    }

    unsafe fn resize(&mut self) {
        let mut l = std::cmp::max(16, self.list.len());
        let next_capacity = self.elems * 3 / 2;
        while l < next_capacity {
            l <<= 1;
        }
        let mut count = 0;
        let mut new_list = vec![null_mut(); l];
        for head in self.list.drain(..) {
            let mut handle = head;
            while !handle.is_null() {
                let idx = (*handle).key as usize & (l - 1);
                let next = (*handle).next_hash;
                (*handle).next_hash = new_list[idx];
                new_list[idx] = handle;
                handle = next;
                count += 1;
            }
        }
        assert_eq!(count, self.elems);
        self.list = new_list;
    }
}

pub struct LRUCacheShard<T> {
    lru: Box<LRUHandle<T>>,
    table: LRUHandleTable<T>,
    object_pool: Vec<Box<LRUHandle<T>>>,
    lru_usage: usize,
    usage: usize,
    capacity: usize,
}
unsafe impl<T> Send for LRUCacheShard<T> {}
unsafe impl<T> Sync for LRUCacheShard<T> {}

impl<T> LRUCacheShard<T> {
    fn new(capacity: usize, object_capacity: usize) -> Self {
        let mut lru = Box::new(LRUHandle::new(0, None));
        lru.prev = lru.as_mut();
        lru.next = lru.as_mut();
        Self {
            capacity,
            lru_usage: 0,
            usage: 0,
            object_pool: Vec::with_capacity(object_capacity),
            lru,
            table: LRUHandleTable::new(),
        }
    }

    unsafe fn lru_remove(&mut self, e: *mut LRUHandle<T>) {
        (*(*e).next).prev = (*e).prev;
        (*(*e).prev).next = (*e).next;
        (*e).prev = null_mut();
        (*e).next = null_mut();
        self.lru_usage -= (*e).charge;
    }

    unsafe fn lru_insert(&mut self, e: *mut LRUHandle<T>) {
        (*e).next = self.lru.as_mut();
        (*e).prev = self.lru.prev;
        (*(*e).prev).next = e;
        (*(*e).next).prev = e;
        self.lru_usage += (*e).charge;
    }

    unsafe fn evict_from_lru(&mut self, charge: usize, last_reference_list: &mut Vec<T>) {
        while self.usage + charge > self.capacity && !std::ptr::eq(self.lru.next, self.lru.as_mut())
        {
            let old_ptr = self.lru.next;
            self.lru_remove(old_ptr);
            self.table.remove((*old_ptr).key);
            (*old_ptr).set_in_cache(false);
            self.usage -= (*old_ptr).charge;
            let mut node = Box::from_raw(old_ptr);
            let data = node.value.take().unwrap();
            last_reference_list.push(data);
            self.recyle_handle_object(node);
        }
    }

    fn recyle_handle_object(&mut self, node: Box<LRUHandle<T>>) {
        if self.object_pool.len() < self.object_pool.capacity() {
            self.object_pool.push(node);
        }
    }

    unsafe fn insert(
        &mut self,
        key: u64,
        charge: usize,
        value: T,
        last_reference_list: &mut Vec<T>,
    ) -> *mut LRUHandle<T> {
        let mut handle = if let Some(mut h) = self.object_pool.pop() {
            h.key = key;
            h.value = Some(value);
            h
        } else {
            Box::new(LRUHandle::new(key, Some(value)))
        };
        handle.charge = charge;
        handle.set_in_cache(true);
        self.evict_from_lru(charge, last_reference_list);
        if self.usage + charge > self.capacity {
            handle.set_in_cache(false);
            let data = handle.value.take().unwrap();
            last_reference_list.push(data);
            self.recyle_handle_object(handle);
            null_mut()
        } else {
            let ptr = Box::into_raw(handle);
            let old = self.table.insert(ptr);
            if !old.is_null() {
                let mut node = Box::from_raw(old);
                node.set_in_cache(false);
                if !node.has_refs() {
                    self.lru_remove(node.as_mut());
                    self.usage -= node.charge;
                    let data = node.value.take().unwrap();
                    last_reference_list.push(data);
                    self.recyle_handle_object(node);
                }
            }
            (*ptr).add_ref();
            ptr
        }
    }

    unsafe fn release(&mut self, e: *mut LRUHandle<T>) -> Option<T> {
        if e.is_null() {
            return None;
        }
        let last_reference = (*e).unref();
        if last_reference && (*e).is_in_cache() {
            if self.usage > self.capacity {
                self.table.remove((*e).key);
                (*e).set_in_cache(false);
            } else {
                self.lru_insert(e);
                return None;
            }
        }
        let mut node = Box::from_raw(e);
        if last_reference {
            self.usage -= node.charge;
            let data = node.value.take().unwrap();
            self.recyle_handle_object(node);
            Some(data)
        } else {
            None
        }
    }

    unsafe fn lookup(&mut self, key: u64) -> *mut LRUHandle<T> {
        let e = self.table.find_pointer(key);
        if !e.is_null() {
            if !(*e).has_refs() {
                self.lru_remove(e);
            }
            (*e).add_ref();
        }
        e
    }
}

pub struct LRUCache<T> {
    num_shard_bits: usize,
    shards: Vec<Mutex<LRUCacheShard<T>>>,
}

impl<T> LRUCache<T> {
    pub fn new(num_shard_bits: usize, capacity: usize, object_cache: usize) -> Self {
        let num_shards = 1 << num_shard_bits;
        let mut shards = Vec::with_capacity(num_shards);
        let per_shard = capacity / num_shards;
        let per_shard_object = object_cache / num_shards;
        for _ in 0..num_shards {
            shards.push(Mutex::new(LRUCacheShard::new(per_shard, per_shard_object)));
        }
        Self {
            shards,
            num_shard_bits,
        }
    }

    pub fn lookup(self: &Arc<Self>, key: u64) -> Option<CachableEntry<T>> {
        let mut shard = self.shards[self.shard(key)].lock();
        unsafe {
            let ptr = shard.lookup(key);
            if ptr.is_null() {
                return None;
            }
            let entry = CachableEntry {
                cache: self.clone(),
                handle: ptr,
            };
            Some(entry)
        }
    }

    pub fn release(&self, handle: *mut LRUHandle<T>) {
        let data = unsafe {
            let mut shard = self.shards[self.shard((*handle).key)].lock();
            shard.release(handle)
        };
        // do not deallocate data with holding mutex.
        drop(data);
    }

    pub fn insert(self: &Arc<Self>, key: u64, charge: usize, value: T) -> Option<CachableEntry<T>> {
        let mut to_delete = vec![];
        let handle = unsafe {
            let mut shard = self.shards[self.shard(key)].lock();
            let ptr = shard.insert(key, charge, value, &mut to_delete);
            if ptr.is_null() {
                None
            } else {
                Some(CachableEntry::<T> {
                    cache: self.clone(),
                    handle: ptr,
                })
            }
        };
        to_delete.clear();
        handle
    }

    fn shard(&self, key: u64) -> usize {
        let hash = key_hash(&key.to_le_bytes());
        if self.num_shard_bits > 0 {
            (hash >> (32 - self.num_shard_bits)) as usize
        } else {
            0
        }
    }
}

pub struct CachableEntry<T> {
    cache: Arc<LRUCache<T>>,
    handle: *mut LRUHandle<T>,
}

unsafe impl<T> Send for CachableEntry<T> {}
unsafe impl<T> Sync for CachableEntry<T> {}

impl<T> CachableEntry<T> {
    pub fn value(&self) -> &T {
        unsafe { (*self.handle).value.as_ref().unwrap() }
    }
}

impl<T> Drop for CachableEntry<T> {
    fn drop(&mut self) {
        self.cache.release(self.handle);
    }
}
