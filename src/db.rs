use crate::common::Result;
use crate::version::{ColumnFamily, VersionSet};
use crate::wal::WalManager;
use crate::write_batch::WriteBatch;
use crate::write_thread::{Writer, WriterQueue};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

pub struct Core {
    versions: VersionSet,
    logfile_number: u64,
}

impl Core {
    fn handle_write_buffer_full(&mut self) {
        let cfs = self.versions.get_column_familys();
        for cf in cfs {
            if !cf.should_flush() {
                break;
            }
            let old_mem = cf.get_memtable();
            old_mem.set_next_log_number(logfile_number);
            let mem = cf.create_memtable();
            let cf = cf.switch_memtable(Arc::new(mem));
            self.versions.set_column_family(Arc::new(cf));
            cf.invalid_column_family();
        }
    }
}

pub struct Engine {
    core: Arc<Mutex<Core>>,
    memtable_write_queue: Arc<Mutex<WriterQueue>>,
    cf_cache: Vec<Arc<ColumnFamily>>,
    wal: Arc<Mutex<WalManager>>,
    last_sequence: Arc<AtomicU64>,
}

impl Engine {
    fn write_impl(&mut self, origin_wb: &mut WriteBatch) -> Result<()> {
        let mut wb = origin_wb.to_raw();
        let count = wb.count();
        let finished = Arc::new(AtomicBool::new(false));
        let (tx, recv) = mpsc::channel();
        let mut sequence = 0;
        {
            let mut wal = self.wal.lock().unwrap();
            sequence = self.last_sequence.fetch_add(count as u64, Ordering::SeqCst);
            wb.set_sequence(sequence);
            let writer = Writer::new(sequence, sequence + count as u64 - 1, finished.clone(), tx);
            let mut queue = self.memtable_write_queue.lock().unwrap();
            queue.join_group(Box::new(writer));
        }
        let mut core = self.core.lock().unwrap();
        if core.versions.should_flush() {
            core.handle_write_buffer_full();
        }
        finished.store(true, Ordering::Release);
        self.memtable_write_queue
            .lock()
            .unwrap()
            .try_catch_all_finished_writers(sequence);
        recv.recv().unwrap();
        Ok(())
    }
}
