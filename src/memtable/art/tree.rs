use crate::memtable::art::node::{
    check_prefix, get_prefix, get_prefix_len, LeafNode, Node, Node16, Node256, Node4, Node48,
};
use crate::memtable::concurrent_arena::SingleArena;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};

const BLOCK_SIZE: usize = 1024;

pub struct ArenaContext {
    arena: SingleArena,
    nodes: Vec<Vec<Node>>,
}

pub struct AdaptiveRadixTree {
    root: AtomicPtr<Node>,
}

impl AdaptiveRadixTree {
    pub fn new() -> Self {
        Self {
            root: AtomicPtr::new(null_mut()),
        }
    }

    pub fn set(&self, ctx: &mut ArenaContext, key: &[u8], value: &[u8]) {
        unsafe {
            let mut current = self.root.load(Ordering::Relaxed);
            if current.is_null() {
                let leaf_node = ctx.add_leaf_node(key, value);
                self.root.store(leaf_node, Ordering::Release);
                return;
            }
            let mut current_ref = &self.root;
            let mut depth = 0;
            loop {
                let prefix_match_len = check_prefix(&*current, &key[depth..]);
                let prefix_len = get_prefix_len(&*current);
                let prefix = get_prefix(&*current);
                let is_prefix_match =
                    std::cmp::min(prefix_len, key.len() - depth) == prefix_match_len;
                if is_prefix_match && prefix_len == key.len() - depth {
                    panic!("we do not allow insert the same key twice");
                }
                if !is_prefix_match {
                    let mut new_parent = Box::from_raw(
                        ctx.arena.allocate(std::mem::size_of::<Node4>()) as *mut Node4,
                    );
                    new_parent.prefix = ctx.arena.allocate(prefix_match_len);
                    std::ptr::copy_nonoverlapping(prefix, new_parent.prefix, prefix_match_len);
                    let new_prefix_len = prefix_len - prefix_match_len - 1;
                    let mut new_prefix = null_mut();
                    if prefix_len > prefix_match_len + 1 {
                        new_prefix = ctx.arena.allocate(new_prefix_len);
                        std::ptr::copy_nonoverlapping(
                            prefix.add(prefix_match_len + 1),
                            new_prefix,
                            new_prefix_len,
                        );
                    }
                    let leaf_offset = depth + prefix_match_len + 1;
                    let leaf_node = ctx.add_leaf_node(&key[leaf_offset..], value);
                    let new_cur = {
                        match &mut *current {
                            Node::LeafNode(n) => {
                                let mut new_node = Box::from_raw(
                                    ctx.arena.allocate(std::mem::size_of::<LeafNode>())
                                        as *mut LeafNode,
                                );
                                new_node.prefix_len = new_prefix_len;
                                new_node.prefix = new_prefix;
                                new_node.value = n.value;
                                Node::LeafNode(new_node)
                            }
                            Node::Node4(n) => {
                                let mut new_node =
                                    Box::from_raw(ctx.arena.allocate(std::mem::size_of::<Node4>())
                                        as *mut Node4);
                                new_node.children_len.store(
                                    n.children_len.load(Ordering::Relaxed),
                                    Ordering::Relaxed,
                                );
                                new_node
                                    .keys
                                    .store(n.keys.load(Ordering::Relaxed), Ordering::Relaxed);
                                new_node.prefix = new_prefix;
                                new_node.prefix_len = new_prefix_len;
                                for i in 0..4 {
                                    new_node.children[i].store(
                                        n.children[i].load(Ordering::Relaxed),
                                        Ordering::Relaxed,
                                    );
                                }
                                Node::Node4(new_node)
                            }
                            Node::Node16(n) => {
                                let mut new_node = Box::from_raw(
                                    ctx.arena.allocate(std::mem::size_of::<Node16>())
                                        as *mut Node16,
                                );
                                new_node.children_len.store(
                                    n.children_len.load(Ordering::Relaxed),
                                    Ordering::Relaxed,
                                );
                                new_node.keys[0]
                                    .store(n.keys[0].load(Ordering::Relaxed), Ordering::Relaxed);
                                new_node.keys[1]
                                    .store(n.keys[1].load(Ordering::Relaxed), Ordering::Relaxed);
                                new_node.prefix = new_prefix;
                                new_node.prefix_len = new_prefix_len;
                                for i in 0..16 {
                                    new_node.children[i].store(
                                        n.children[i].load(Ordering::Relaxed),
                                        Ordering::Relaxed,
                                    );
                                }
                                Node::Node16(new_node)
                            }
                            Node::Node48(n) => {
                                let mut new_node = Box::from_raw(
                                    ctx.arena.allocate(std::mem::size_of::<Node48>())
                                        as *mut Node48,
                                );
                                new_node.children_len.store(0, Ordering::Relaxed);
                                new_node.prefix = new_prefix;
                                new_node.prefix_len = new_prefix_len;
                                n.iter_child(|c, child| {
                                    new_node.set_child(c, child);
                                });
                                Node::Node48(new_node)
                            }
                            Node::Node256(n) => {
                                let mut new_node = Box::from_raw(
                                    ctx.arena.allocate(std::mem::size_of::<Node256>())
                                        as *mut Node256,
                                );
                                new_node.children_len.store(0, Ordering::Relaxed);
                                new_node.prefix = new_prefix;
                                new_node.prefix_len = new_prefix_len;
                                n.iter_child(|c, child| {
                                    new_node.set_child(c, child);
                                });
                                Node::Node256(new_node)
                            }
                        }
                    };
                    new_parent.set_child(*prefix.add(prefix_match_len), ctx.add_node(new_cur));
                    new_parent.set_child(key[depth + prefix_match_len], leaf_node);
                    current_ref.store(ctx.add_node(Node::Node4(new_parent)), Ordering::Release);
                    return;
                }
                let child_partial_key = key[depth + prefix_len];
                let child_pref = (*current).get_child_ref(child_partial_key);
                if let Some(child_ptr) = child_pref {
                    let child = child_ptr.load(Ordering::Relaxed);
                    if !child.is_null() {
                        depth += prefix_len + 1;
                        current = child;
                        current_ref = child_ptr;
                        continue;
                    }
                }
                let leaf_prefix_addr = depth + prefix_len + 1;
                let new_leaf = ctx.add_leaf_node(&key[leaf_prefix_addr..], value);
                match &mut *current {
                    Node::LeafNode(_) => unreachable!("can not be leaf node"),
                    Node::Node4(n) => {
                        if n.is_full() {
                            let mut new_node =
                                Box::from_raw(ctx.arena.allocate(std::mem::size_of::<Node16>())
                                    as *mut Node16);
                            new_node.prefix = n.prefix;
                            new_node.prefix_len = n.prefix_len;
                            new_node.children_len.store(0, Ordering::Relaxed);
                            n.iter_child(|c, child| {
                                new_node.set_child(c, child);
                            });
                            new_node.set_child(child_partial_key, new_leaf);
                            current = ctx.add_node(Node::Node16(new_node));
                            current_ref.store(current, Ordering::Release);
                        } else {
                            n.set_child(child_partial_key, new_leaf);
                        }
                    }
                    Node::Node16(n) => {
                        if n.is_full() {
                            let mut new_node =
                                Box::from_raw(ctx.arena.allocate(std::mem::size_of::<Node48>())
                                    as *mut Node48);
                            new_node.prefix = n.prefix;
                            new_node.prefix_len = n.prefix_len;
                            new_node.children_len.store(0, Ordering::Relaxed);
                            n.iter_child(|c, child| {
                                new_node.set_child(c, child);
                            });
                            new_node.set_child(child_partial_key, new_leaf);
                            current = ctx.add_node(Node::Node48(new_node));
                            current_ref.store(current, Ordering::Release);
                        } else {
                            n.set_child(child_partial_key, new_leaf);
                        }
                    }
                    Node::Node48(n) => {
                        if n.is_full() {
                            let mut new_node =
                                Box::from_raw(ctx.arena.allocate(std::mem::size_of::<Node256>())
                                    as *mut Node256);
                            new_node.prefix = n.prefix;
                            new_node.prefix_len = n.prefix_len;
                            new_node.children_len.store(0, Ordering::Relaxed);
                            n.iter_child(|c, child| {
                                new_node.set_child(c, child);
                            });
                            new_node.set_child(child_partial_key, new_leaf);
                            current = ctx.add_node(Node::Node256(new_node));
                            current_ref.store(current, Ordering::Release);
                        } else {
                            n.set_child(child_partial_key, new_leaf);
                        }
                    }
                    Node::Node256(n) => {
                        n.set_child(child_partial_key, new_leaf);
                    }
                }
                return;
            }
        }
    }
}

impl ArenaContext {
    unsafe fn add_node(&mut self, node: Node) -> *mut Node {
        if self.nodes.is_empty() || self.nodes.last().unwrap().len() >= BLOCK_SIZE {
            self.nodes.push(Vec::with_capacity(BLOCK_SIZE));
        }
        let nodes = self.nodes.last_mut().unwrap();
        nodes.push(node);
        nodes.last_mut().unwrap()
    }

    unsafe fn add_leaf_node(&mut self, key: &[u8], value: &[u8]) -> *mut Node {
        if self.nodes.is_empty() || self.nodes.last().unwrap().len() >= BLOCK_SIZE {
            self.nodes.push(Vec::with_capacity(BLOCK_SIZE));
        }
        let addr = self
            .arena
            .allocate(std::mem::size_of::<LeafNode>() + key.len() + value.len() + 4);
        let key_ptr = addr.add(std::mem::size_of::<LeafNode>());
        let mut node = Box::from_raw(addr as *mut LeafNode);
        node.prefix = key_ptr;
        node.prefix_len = key.len();
        node.value = key_ptr.add(key.len());
        std::ptr::copy_nonoverlapping((value.len() as u32).to_le_bytes().as_ptr(), node.value, 4);
        std::ptr::copy_nonoverlapping(value.as_ptr(), node.value.add(4), value.len());
        let nodes = self.nodes.last_mut().unwrap();
        nodes.push(Node::LeafNode(node));
        nodes.last_mut().unwrap()
    }
}
