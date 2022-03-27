use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicU8, Ordering};

pub enum Node {
    LeafNode(Box<LeafNode>),
    Node4(Box<Node4>),
    Node16(Box<Node16>),
    Node48(Box<Node48>),
    Node256(Box<Node256>),
}

impl Node {
    pub unsafe fn get_child_ref(&self, c: u8) -> Option<&AtomicPtr<Node>> {
        match self {
            Node::Node4(n) => n.get_child_ref(c),
            Node::Node16(n) => n.get_child_ref(c),
            Node::Node48(n) => n.get_child_ref(c),
            Node::Node256(n) => n.get_child_ref(c),
            Node::LeafNode(_) => None,
        }
    }

    pub unsafe fn get_child(&self, c: u8) -> *mut Node {
        match self {
            Node::Node4(n) => n.get_child(c),
            Node::Node16(n) => n.get_child(c),
            Node::Node48(n) => n.get_child(c),
            Node::Node256(n) => n.get_child(c),
            Node::LeafNode(_) => null_mut(),
        }
    }
}

fn check_slice_prefix(a: &[u8], b: &[u8]) -> usize {
    let l = std::cmp::min(a.len(), b.len());
    for i in 0..l {
        if a[i] != b[i] {
            return i;
        }
    }
    l
}

pub unsafe fn check_prefix(node: &Node, key: &[u8]) -> usize {
    match node {
        Node::LeafNode(n) => {
            check_slice_prefix(std::slice::from_raw_parts(n.prefix, n.prefix_len), key)
        }
        Node::Node4(n) => {
            check_slice_prefix(std::slice::from_raw_parts(n.prefix, n.prefix_len), key)
        }
        Node::Node16(n) => {
            check_slice_prefix(std::slice::from_raw_parts(n.prefix, n.prefix_len), key)
        }
        Node::Node48(n) => {
            check_slice_prefix(std::slice::from_raw_parts(n.prefix, n.prefix_len), key)
        }
        Node::Node256(n) => {
            check_slice_prefix(std::slice::from_raw_parts(n.prefix, n.prefix_len), key)
        }
    }
}

pub fn get_prefix_len(node: &Node) -> usize {
    match node {
        Node::LeafNode(n) => n.prefix_len,
        Node::Node4(n) => n.prefix_len,
        Node::Node16(n) => n.prefix_len,
        Node::Node48(n) => n.prefix_len,
        Node::Node256(n) => n.prefix_len,
    }
}

pub unsafe fn get_prefix(node: &Node) -> *mut u8 {
    match node {
        Node::LeafNode(n) => n.prefix,
        Node::Node4(n) => n.prefix,
        Node::Node16(n) => n.prefix,
        Node::Node48(n) => n.prefix,
        Node::Node256(n) => n.prefix,
    }
}

#[repr(C)]
pub struct LeafNode {
    pub prefix_len: usize,
    pub prefix: *mut u8,
    pub value: *mut u8,
}

#[repr(C)]
pub struct Node4 {
    pub children_len: AtomicU8,
    pub keys: AtomicU32,
    pub children: [AtomicPtr<Node>; 4],
    pub prefix_len: usize,
    pub prefix: *mut u8,
}

impl Node4 {
    pub unsafe fn is_full(&self) -> bool {
        self.children_len.load(Ordering::Acquire) == 4
    }

    pub unsafe fn set_child(&self, c: u8, child: *mut Node) {
        let idx = self.children_len.load(Ordering::Relaxed);
        let idx_value = (c as u32) << (idx * 8);
        let key = self.keys.load(Ordering::Relaxed);
        self.keys.store(key | idx_value, Ordering::Release);
        self.children[idx as usize].store(child, Ordering::Release);
        self.children_len.store(idx + 1, Ordering::Release);
    }

    pub unsafe fn get_child(&self, c: u8) -> *mut Node {
        // must load children_len at first.
        let idx = self.children_len.load(Ordering::Acquire);
        let key = self.keys.load(Ordering::Acquire);
        for i in 0..idx {
            if ((key >> (i * 8)) & 255) == c as u32 {
                return self.children[i as usize].load(Ordering::Acquire);
            }
        }
        null_mut()
    }
    pub unsafe fn get_child_ref(&self, c: u8) -> Option<&AtomicPtr<Node>> {
        // must load children_len at first.
        let idx = self.children_len.load(Ordering::Acquire);
        let key = self.keys.load(Ordering::Acquire);
        for i in 0..idx {
            if ((key >> (i * 8)) & 255) == c as u32 {
                return Some(&self.children[i as usize]);
            }
        }
        None
    }

    pub unsafe fn iter_child<F: Fn(u8, *mut Node)>(&self, f: F) {
        let max_idx = self.children_len.load(Ordering::Relaxed) as usize;
        let key = self.keys.load(Ordering::Relaxed);
        for children_idx in 0..max_idx {
            let c = ((key as usize >> (children_idx * 8)) & 255) as u8;
            let ptr = self.children[children_idx].load(Ordering::Acquire);
            if !ptr.is_null() {
                f(c, ptr);
            }
        }
    }
}

#[repr(C)]
pub struct Node16 {
    pub children_len: AtomicU8,
    pub keys: [AtomicU64; 2],
    pub children: [AtomicPtr<Node>; 16],
    pub prefix_len: usize,
    pub prefix: *mut u8,
}

impl Node16 {
    pub unsafe fn is_full(&self) -> bool {
        self.children_len.load(Ordering::Acquire) == 16
    }

    pub unsafe fn get_child(&self, c: u8) -> *mut Node {
        // must load children_len at first.
        let idx = self.children_len.load(Ordering::Acquire);
        let key = self.keys[0].load(Ordering::Acquire);
        let key1_len = std::cmp::min(idx, 8);
        for i in 0..key1_len {
            if ((key >> (i * 8)) & 255) == c as u64 {
                return self.children[i as usize].load(Ordering::Acquire);
            }
        }
        if idx > 4 {
            let key = self.keys[1].load(Ordering::Acquire);
            let key1_len = idx - 4;
            for i in 0..key1_len {
                if ((key >> (i * 8)) & 255) == c as u64 {
                    return self.children[(4 + i) as usize].load(Ordering::Acquire);
                }
            }
        }
        null_mut()
    }
    pub unsafe fn get_child_ref(&self, c: u8) -> Option<&AtomicPtr<Node>> {
        // must load children_len at first.
        let idx = self.children_len.load(Ordering::Acquire);
        let key = self.keys[0].load(Ordering::Acquire);
        let key1_len = std::cmp::min(idx, 8);
        for i in 0..key1_len {
            if ((key >> (i * 8)) & 255) == c as u64 {
                return Some(&self.children[i as usize]);
            }
        }
        if idx > 4 {
            let key = self.keys[1].load(Ordering::Acquire);
            let key1_len = idx - 4;
            for i in 0..key1_len {
                if ((key >> (i * 8)) & 255) == c as u64 {
                    return Some(&self.children[(4 + i) as usize]);
                }
            }
        }
        None
    }

    pub unsafe fn set_child(&self, c: u8, child: *mut Node) {
        let idx = self.children_len.load(Ordering::Relaxed);
        if c > 0 {
            if idx < 8 {
                let idx_value = (c as u64) << (idx * 8);
                let key = self.keys[0].load(Ordering::Relaxed);
                self.keys[0].store(key | idx_value, Ordering::Release);
            } else {
                let idx_value = (c as u64) << ((idx - 8) * 8);
                let key = self.keys[1].load(Ordering::Relaxed);
                self.keys[1].store(key | idx_value, Ordering::Release);
            }
        }
        self.children[idx as usize].store(child, Ordering::Release);
        self.children_len.store(idx + 1, Ordering::Release);
    }

    pub unsafe fn iter_child<F: Fn(u8, *mut Node)>(&self, f: F) {
        let mut children_len = self.children_len.load(Ordering::Relaxed) as usize;
        for i in 0..2 {
            let key = self.keys[i].load(Ordering::Acquire);
            let l = std::cmp::min(8, children_len);
            for children_idx in 0..l {
                let c = ((key as usize >> (children_idx * 8)) & 255) as u8;
                let ptr = self.children[i * 8 + children_idx].load(Ordering::Acquire);
                if !ptr.is_null() {
                    f(c, ptr);
                }
            }
            if children_len >= 8 {
                children_len -= 8;
            } else {
                return;
            }
        }
    }
}

#[repr(C)]
pub struct Node48 {
    pub children_len: AtomicU8,
    pub keys: [AtomicU64; 32],
    pub children: [AtomicPtr<Node>; 48],
    pub prefix_len: usize,
    pub prefix: *mut u8,
}

impl Node48 {
    pub unsafe fn is_full(&self) -> bool {
        self.children_len.load(Ordering::Acquire) == 48
    }

    pub unsafe fn get_child(&self, c: u8) -> *mut Node {
        // must load children_len at first.
        let c_idx = c >> 3;
        let c_bit = c & 7;
        let key = self.keys[c_idx as usize].load(Ordering::Acquire);
        let children_idx = (key as usize >> (c_bit as usize * 8)) & 255;
        if children_idx > 0 {
            return self.children[children_idx - 1].load(Ordering::Acquire);
        }
        null_mut()
    }
    pub unsafe fn get_child_ref(&self, c: u8) -> Option<&AtomicPtr<Node>> {
        // must load children_len at first.
        let c_idx = c >> 3;
        let c_bit = c & 7;
        let key = self.keys[c_idx as usize].load(Ordering::Acquire);
        let children_idx = (key as usize >> (c_bit as usize * 8)) & 255;
        if children_idx > 0 {
            return Some(&self.children[children_idx - 1]);
        }
        None
    }

    pub unsafe fn iter_child<F: Fn(u8, *mut Node)>(&self, f: F) {
        for i in 0..32 {
            let key = self.keys[i].load(Ordering::Acquire);
            if key == 0 {
                continue;
            }
            for c_bit in 0..8 {
                let children_idx = (key as usize >> (c_bit as usize * 8)) & 255;
                let c = (i as u8) << 3 | c_bit;
                if children_idx > 0 {
                    f(c, self.children[children_idx - 1].load(Ordering::Acquire));
                }
            }
        }
    }

    pub unsafe fn set_child(&self, c: u8, n: *mut Node) {
        // must load children_len at first.
        let c_idx = (c >> 3) as usize;
        let c_bit = c & 7;
        let children_idx = self.children_len.load(Ordering::Relaxed) + 1;
        let key = self.keys[c_idx].load(Ordering::Acquire);
        // store pos + 1 because we use 0 present null.
        self.keys[c_idx].store(
            key | (children_idx as u64) << (c_bit as u64 * 8),
            Ordering::Release,
        );
        self.children[children_idx as usize - 1].store(n, Ordering::Release);
        self.children_len.store(children_idx, Ordering::Release);
    }
}

#[repr(C)]
pub struct Node256 {
    pub children_len: AtomicU8,
    pub children: [AtomicPtr<Node>; 256],
    pub prefix_len: usize,
    pub prefix: *mut u8,
}

impl Node256 {
    pub unsafe fn is_full(&self) -> bool {
        self.children_len.load(Ordering::Acquire) == 255
    }

    pub unsafe fn get_child(&self, c: u8) -> *mut Node {
        self.children[c as usize].load(Ordering::Acquire)
    }
    pub unsafe fn set_child(&self, c: u8, child: *mut Node) {
        self.children[c as usize].store(child, Ordering::Release);
    }
    pub unsafe fn get_child_ref(&self, c: u8) -> Option<&AtomicPtr<Node>> {
        Some(&self.children[c as usize])
    }
    pub unsafe fn iter_child<F: Fn(u8, *mut Node)>(&self, f: F) {
        for i in 0..256 {
            let ptr = self.children[i].load(Ordering::Acquire);
            if ptr.is_null() {
                f(i as u8, ptr);
            }
        }
    }
}
