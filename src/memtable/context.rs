use crate::memtable::Splice;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    pub static CACHE_ID: RefCell<usize> = RefCell::new(0);
}

lazy_static::lazy_static! {
    static ref GLOBAL_CACHE_ID: AtomicUsize = AtomicUsize::new(1);
}

#[derive(Default, Clone)]
pub struct MemTableContext {
    pub(crate) splice: Splice,
    thread_id: usize,
    // TODO: Support allocate data from local thread arena.
}

impl MemTableContext {
    pub fn get_thread_id(&mut self) -> usize {
        if self.thread_id == 0 {
            let idx = CACHE_ID.with(|x| {
                if *x.borrow() != 0 {
                    return *x.borrow();
                }
                *x.borrow_mut() = GLOBAL_CACHE_ID.fetch_add(1, Ordering::SeqCst);
                *x.borrow()
            });
            self.thread_id = idx;
        }
        self.thread_id
    }
}
