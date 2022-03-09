use spin::Mutex;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const BLOCK_DATA_SIZE: usize = 2 * 1024 * 1024;
const PAGE_DATA_SIZE: usize = 8 * 1024;

struct Block {
    data: Vec<u8>,
    offset: AtomicUsize,
}

pub struct Arena {
    blocks: Vec<Box<Block>>,
    current: Box<Block>,
}

impl Arena {
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

pub struct ConcurrentArena {
    arena: Mutex<Arena>,
    current: AtomicPtr<Block>,
    mem_size: AtomicUsize,
}

impl ConcurrentArena {
    pub fn new() -> Self {
        let mut arena = Arena::new();
        ConcurrentArena {
            current: AtomicPtr::new(arena.current.as_mut()),
            arena: Mutex::new(arena),
            mem_size: AtomicUsize::new(0),
        }
    }

    pub fn mem_size(&self) -> usize {
        self.mem_size.load(Ordering::Relaxed)
    }

    pub unsafe fn allocate(&self, alloc_size: usize) -> *mut u8 {
        let data_size = ((alloc_size - 1) | (std::mem::size_of::<*mut u8>() - 1)) + 1;
        let current = self.current.load(Ordering::Acquire);
        if (*current).offset.load(Ordering::Acquire) + data_size < BLOCK_DATA_SIZE {
            let offset = (*current).offset.fetch_add(data_size, Ordering::SeqCst);
            if offset + data_size < BLOCK_DATA_SIZE {
                return (*current).data.as_mut_ptr().add(offset) as _;
            }
        }
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
        let block = Box::new(Block {
            data: Vec::with_capacity(block_size),
            offset: AtomicUsize::new(data_size),
        });
        let old = std::mem::replace(&mut arena.current, block);
        self.mem_size
            .fetch_add(old.data.capacity(), Ordering::Relaxed);
        arena.blocks.push(old);
        return arena.current.data.as_mut_ptr();
    }
}
