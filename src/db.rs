use crate::common::{InternalKeyComparator, Result};
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::table::InternalIterator;
use crate::version::{ColumnFamily, VersionEdit, VersionSet, VersionSetKernal};
use crate::write_batch::WriteBatch;
use futures::channel::mpsc::Sender;

use crate::compaction::CompactionEngine;
use crate::version::manifest::Manifest;
use std::sync::atomic::AtomicU64;
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
            // let old_mem = cf.get_memtable();
            // old_mem.set_next_log_number(logfile_number);
            let mem = cf.create_memtable();
            let new_cf = cf.switch_memtable(Arc::new(mem));
            self.versions.set_column_family(Arc::new(new_cf));
            cf.invalid_column_family();
        }
    }
}

#[derive(Clone)]
pub struct Engine {
    core: Arc<Mutex<Core>>,
    cf_cache: Vec<Arc<ColumnFamily>>,
    kernal: Arc<VersionSetKernal>,
}

impl Engine {
    fn write_impl(&mut self, _: &mut WriteBatch) -> Result<()> {
        Ok(())
    }
}
