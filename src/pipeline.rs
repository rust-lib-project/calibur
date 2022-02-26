use crate::version::KernelNumberContext;
use std::sync::{Arc, Condvar, Mutex};

/// PipelineCommitQueue will make the sequence of writebatch can be commit as their sequence order.

struct QueueState {
    waiter_ount: usize,
    stop: bool,
}

pub struct PipelineCommitQueue {
    cond: Condvar,
    waiters: Mutex<QueueState>,
    kernel: Arc<KernelNumberContext>,
}

impl PipelineCommitQueue {
    pub fn new(kernel: Arc<KernelNumberContext>) -> Self {
        Self {
            cond: Condvar::new(),
            waiters: Mutex::new(QueueState {
                waiter_ount: 0,
                stop: false,
            }),
            kernel,
        }
    }

    pub fn commit(&self, last_commit_sequence: u64, commit_sequence: u64) {
        // We do not need to use compare_exchange because every request will have its own unique
        //  `commit_sequence` and `last_commit_sequence`.
        for _ in 0..100 {
            if self.kernel.last_sequence() == last_commit_sequence {
                self.kernel.set_last_sequence(commit_sequence);
                self.cond.notify_all();
                return;
            }
        }
        let mut state = self.waiters.lock().unwrap();
        if self.kernel.last_sequence() == last_commit_sequence {
            self.kernel.set_last_sequence(commit_sequence);
            self.cond.notify_all();
            return;
        }
        while self.kernel.last_sequence() != last_commit_sequence && !state.stop {
            state = self.cond.wait(state).unwrap();
        }
        if !state.stop {
            self.kernel.set_last_sequence(commit_sequence);
            self.cond.notify_all();
        }
    }

    // return true means this commit queue has been stopped and the DB will be closed.
    pub fn wait_pending_writers(&self, commit_sequence: u64) -> bool {
        for _ in 0..100 {
            if self.kernel.last_sequence() >= commit_sequence {
                return false;
            }
        }
        let mut state = self.waiters.lock().unwrap();
        while self.kernel.last_sequence() < commit_sequence && !state.stop {
            state = self.cond.wait(state).unwrap();
        }
        state.stop
    }

    pub fn stop(&mut self) {
        let mut state = self.waiters.lock().unwrap();
        state.stop = true;
        self.cond.notify_all();
    }
}
