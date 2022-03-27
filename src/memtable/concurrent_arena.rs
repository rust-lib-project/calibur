use spin::Mutex;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const BLOCK_DATA_SIZE: usize = 4 * 1024 * 1024;
const PAGE_DATA_SIZE: usize = 8 * 1024;

struct Block {
    data: Vec<u8>,
    offset: AtomicUsize,
}

impl Block {
    pub fn new(data_size: usize) -> Box<Self> {
        let mut block_size = BLOCK_DATA_SIZE;
        while block_size < data_size {
            if block_size + PAGE_DATA_SIZE < data_size {
                block_size += (data_size - block_size) / PAGE_DATA_SIZE * PAGE_DATA_SIZE;
            } else {
                block_size += PAGE_DATA_SIZE;
            }
        }
        Box::new(Block {
            data: Vec::with_capacity(block_size),
            offset: AtomicUsize::new(data_size),
        })
    }
}

pub struct ArenaContent {
    blocks: Vec<Box<Block>>,
    current: Box<Block>,
}

impl ArenaContent {
    pub fn create_block(&mut self, data_size: usize) -> *mut u8 {
        let block = Block::new(data_size);
        let old = std::mem::replace(&mut self.current, block);
        self.blocks.push(old);
        self.current.data.as_mut_ptr()
    }
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
    unsafe fn allocate_in_thread(&self, _: usize, alloc_size: usize) -> *mut u8 {
        self.allocate(alloc_size)
    }
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
        let offset = (*current).offset.fetch_add(data_size, Ordering::SeqCst);
        if offset + data_size < BLOCK_DATA_SIZE {
            return (*current).data.as_mut_ptr().add(offset) as _;
        }
        return null_mut();
    }

    unsafe fn allocate_heap(&self, data_size: usize, mem_size: &AtomicUsize) -> *mut u8 {
        let mut arena = self.arena.lock();
        let offset = arena.current.offset.fetch_add(data_size, Ordering::SeqCst);
        if offset + data_size < BLOCK_DATA_SIZE {
            return arena.current.data.as_mut_ptr().add(offset) as _;
        }
        mem_size.fetch_add(arena.current.data.capacity(), Ordering::Relaxed);
        let addr = arena.create_block(data_size);
        self.current
            .store(arena.current.as_mut(), Ordering::Release);
        addr
    }
}
pub struct SingleArena {
    content: ArenaContent,
    mem_size: AtomicUsize,
}

impl SingleArena {
    pub fn new() -> Self {
        SingleArena {
            content: ArenaContent::new(),
            mem_size: AtomicUsize::new(0),
        }
    }

    pub unsafe fn allocate(&mut self, alloc_size: usize) -> *mut u8 {
        let data_size = ((alloc_size - 1) | (std::mem::size_of::<*mut u8>() - 1)) + 1;
        let offset = self
            .content
            .current
            .offset
            .fetch_add(data_size, Ordering::SeqCst);
        if offset + data_size < BLOCK_DATA_SIZE {
            return self.content.current.data.as_mut_ptr().add(offset) as _;
        }
        self.mem_size
            .fetch_add(self.content.current.data.capacity(), Ordering::Relaxed);
        self.content.create_block(data_size)
    }

    fn mem_size(&self) -> usize {
        self.content.current.offset.load(Ordering::Relaxed) + self.mem_size.load(Ordering::Relaxed)
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

const ARENA_COUNT: usize = 4;

pub struct SharedArena {
    arenas: Vec<ArenaShard>,
    mem_size: AtomicUsize,
    id: AtomicUsize,
}

impl SharedArena {
    pub fn new() -> Self {
        let mut arenas = vec![];
        for _ in 0..ARENA_COUNT {
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
        let arena = &self.arenas[0];
        let addr = arena.allocate_from_current_block(data_size);
        if !addr.is_null() {
            return addr;
        }
        arena.allocate_heap(data_size, &self.mem_size)
    }

    unsafe fn allocate_in_thread(&self, idx: usize, alloc_size: usize) -> *mut u8 {
        let data_size = ((alloc_size - 1) | (std::mem::size_of::<*mut u8>() - 1)) + 1;
        let arena = &self.arenas[idx % ARENA_COUNT];
        let addr = arena.allocate_from_current_block(data_size);
        if !addr.is_null() {
            return addr;
        }
        arena.allocate_heap(data_size, &self.mem_size)
    }
}
