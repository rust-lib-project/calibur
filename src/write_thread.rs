use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{mpsc, Arc, Mutex};

pub struct Writer {
    pub sequence: u64,
    pub commit_sequence: u64,
    pub finished: Arc<AtomicBool>,
    pub sender: mpsc::Sender<u64>,
}

impl Writer {
    pub fn new(
        sequence: u64,
        commit_sequence: u64,
        finished: Arc<AtomicBool>,
        sender: mpsc::Sender<u64>,
    ) -> Writer {
        Writer {
            sequence,
            finished,
            sender,
            commit_sequence,
        }
    }
}

pub struct WriterQueue {
    writers: VecDeque<Box<Writer>>,
    commit_sequence: Arc<AtomicU64>,
}

impl WriterQueue {
    pub fn join_group(&mut self, writer: Box<Writer>) {
        self.writers.push_back(writer);
    }

    pub fn try_catch_all_finished_writers(&mut self, sequence: u64) {
        // assert!(!self.writers.is_empty());
        // if self.writers.front().unwrap().sequence != sequence {
        //     return;
        // }
        // let mut writers = vec![];
        // let mut commit_sequence = sequence;
        // while let Some(writer) = self.writers.pop_front() {
        //     if !writer.finished.load(Ordering::Acquire) {
        //         self.writers.push_front(writer);
        //         break;
        //     }
        //     commit_sequence = writer.commit_sequence;
        //     writers.push(writer);
        // }
        // self.commit_sequence
        //     .store(commit_sequence, Ordering::Release);
        // for w in writers {
        //     w.sender.send(w.commit_sequence);
        // }
    }
}
