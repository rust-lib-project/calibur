use std::ptr::null_mut;

pub struct SnapshotList {
    head: Box<Snapshot>,
    count: usize,
}

impl SnapshotList {
    pub fn new() -> Self {
        let mut head = Box::new(Snapshot {
            next: null_mut(),
            prev: null_mut(),
            sequence: 0,
        });
        head.next = head.as_mut();
        head.prev = head.as_mut();
        Self { head, count: 0 }
    }

    pub fn new_snapshot(&mut self, sequence: u64) -> Box<Snapshot> {
        let mut s = Box::new(Snapshot {
            next: self.head.as_mut(),
            prev: self.head.prev,
            sequence,
        });
        unsafe {
            (*s.prev).next = s.as_mut();
            (*s.next).prev = s.as_mut();
        }
        self.count += 1;
        s
    }

    pub fn release_snapshot(&mut self, s: Box<Snapshot>) {
        unsafe {
            (*s.prev).next = s.next;
            (*s.next).prev = s.prev;
        }

        self.count -= 1;
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn collect_snapshots(&mut self, snapshots: &mut Vec<u64>) {
        unsafe {
            let mut s: *mut Snapshot = self.head.as_mut();
            while (*s).sequence != 0 {
                snapshots.push((*s).sequence);
                s = (*s).next;
            }
        }
    }
}

pub struct Snapshot {
    next: *mut Snapshot,
    prev: *mut Snapshot,
    sequence: u64,
}

impl Snapshot {
    pub fn get_sequence(&self) -> u64 {
        self.sequence
    }
}

unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}
