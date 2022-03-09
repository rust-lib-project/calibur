use spin::Mutex;
use std::cell::RefCell;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const BLOCK_DATA_SIZE: usize = 2 * 1024 * 1024;
const PAGE_DATA_SIZE: usize = 8 * 1024;

thread_local! {
    pub static CACHE_ID: RefCell<usize> = RefCell::new(0);
}

struct Block {
    data: Vec<u8>,
    offset: AtomicUsize,
}

pub struct ArenaContent {
    blocks: Vec<Box<Block>>,
    current: Box<Block>,
}

impl ArenaContent {
    pub fn new() -> Self {
        Self {
            blocks: vec![],
            current: Box::new(Block {
                data: Vec::with_capacity(BLOCK_DATA_SIZE),
                offset: AtomicUsize::new(0),
            }),
        }
    }
}

pub trait Arena {
    fn mem_size(&self) -> usize;
    unsafe fn allocate(&self, alloc_size: usize) -> *mut u8;
}

pub struct ArenaShard {
    arena: Mutex<ArenaContent>,
    current: AtomicPtr<Block>,
}

impl Default for ArenaShard {
    fn default() -> Self {
        let mut arena = ArenaContent::new();
        ArenaShard {
            current: AtomicPtr::new(arena.current.as_mut()),
            arena: Mutex::new(arena),
        }
    }
}

impl ArenaShard {
    unsafe fn allocate_from_current_block(&self, data_size: usize) -> *mut u8 {
        let current = self.current.load(Ordering::Acquire);
        if (*current).offset.load(Ordering::Acquire) + data_size < BLOCK_DATA_SIZE {
            let offset = (*current).offset.fetch_add(data_size, Ordering::SeqCst);
            if offset + data_size < BLOCK_DATA_SIZE {
                return (*current).data.as_mut_ptr().add(offset) as _;
            }
        }
        return null_mut();
    }

    unsafe fn allocate_heap(&self, data_size: usize, mem_size: &AtomicUsize) -> *mut u8 {
        let mut arena = self.arena.lock();
        if arena.current.offset.load(Ordering::Acquire) + data_size < BLOCK_DATA_SIZE {
            let offset = arena.current.offset.fetch_add(data_size, Ordering::SeqCst);
            if offset + data_size < BLOCK_DATA_SIZE {
                return arena.current.data.as_mut_ptr().add(offset) as _;
            }
        }

        let mut block_size = BLOCK_DATA_SIZE;
        while block_size < data_size {
            if block_size + PAGE_DATA_SIZE < data_size {
                block_size += (data_size - block_size) / PAGE_DATA_SIZE * PAGE_DATA_SIZE;
            } else {
                block_size += PAGE_DATA_SIZE;
            }
        }
        let mut block = Box::new(Block {
            data: Vec::with_capacity(block_size),
            offset: AtomicUsize::new(data_size),
        });
        self.current.store(block.as_mut(), Ordering::Release);
        let old = std::mem::replace(&mut arena.current, block);
        mem_size.fetch_add(old.data.capacity(), Ordering::Relaxed);
        arena.blocks.push(old);
        return arena.current.data.as_mut_ptr();
    }
}

pub struct ConcurrentArena {
    arena: ArenaShard,
    mem_size: AtomicUsize,
}

impl ConcurrentArena {
    pub fn new() -> Self {
        ConcurrentArena {
            arena: ArenaShard::default(),
            mem_size: AtomicUsize::new(0),
        }
    }
}

impl Arena for ConcurrentArena {
    fn mem_size(&self) -> usize {
        self.mem_size.load(Ordering::Relaxed)
    }

    unsafe fn allocate(&self, alloc_size: usize) -> *mut u8 {
        let data_size = ((alloc_size - 1) | (std::mem::size_of::<*mut u8>() - 1)) + 1;
        let addr = self.arena.allocate_from_current_block(data_size);
        if !addr.is_null() {
            return addr;
        }
        self.arena.allocate_heap(data_size, &self.mem_size)
    }
}

pub struct SharedArena {
    arenas: Vec<ArenaShard>,
    mem_size: AtomicUsize,
    id: AtomicUsize,
}

impl SharedArena {
    pub fn new() -> Self {
        let mut arenas = vec![];
        for _ in 0..8 {
            arenas.push(ArenaShard::default());
        }
        SharedArena {
            arenas,
            mem_size: AtomicUsize::new(0),
            id: AtomicUsize::new(1),
        }
    }
}

impl Arena for SharedArena {
    fn mem_size(&self) -> usize {
        self.mem_size.load(Ordering::Acquire)
    }

    unsafe fn allocate(&self, alloc_size: usize) -> *mut u8 {
        let data_size = ((alloc_size - 1) | (std::mem::size_of::<*mut u8>() - 1)) + 1;
        let idx: usize = CACHE_ID.with(|x| {
            if *x.borrow() != 0 {
                return *x.borrow();
            }
            *x.borrow_mut() = self.id.fetch_add(1, Ordering::SeqCst);
            *x.borrow()
        });
        let arena = &self.arenas[idx % 8];
        let addr = arena.allocate_from_current_block(data_size);
        if !addr.is_null() {
            return addr;
        }
        arena.allocate_heap(data_size, &self.mem_size)
    }
}
