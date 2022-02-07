use crate::common::{InternalKeyComparator, Result};
use crate::options::ImmutableDBOptions;
use crate::version::{ColumnFamily, VersionSet, VersionSetKernel};
use futures::channel::mpsc::unbounded;
use std::collections::HashMap;

use crate::manifest::{ManifestEngine, ManifestWriter};
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use yatp::{task::future::TaskCell, ThreadPool};

pub struct Core {
    versions: VersionSet,
}

impl Core {
    // fn handle_write_buffer_full(&mut self) {
    //     let cfs = self.versions.get_column_familys();
    //     for cf in cfs {
    //         if !cf.should_flush() {
    //             break;
    //         }
    //         let old_mem = cf.get_memtable();
    //         old_mem.set_next_log_number(logfile_number);
    //         let mem = cf.create_memtable();
    //         let new_cf = cf.switch_memtable(Arc::new(mem));
    //         self.versions.set_column_family(Arc::new(new_cf));
    //         cf.invalid_column_family();
    //     }
    // }
}

#[derive(Clone)]
pub struct Engine {
    version_set: Arc<Mutex<VersionSet>>,
    cf_cache: Vec<Arc<ColumnFamily>>,
    kernel: Arc<VersionSetKernel>,
    pool: Arc<ThreadPool<TaskCell>>,
    options: Arc<ImmutableDBOptions>,
}

impl Engine {
    fn run_manifest_job(
        version_set_with_lock: Arc<Mutex<VersionSet>>,
        options: Arc<ImmutableDBOptions>,
        pool: &ThreadPool<TaskCell>,
        comparator: InternalKeyComparator,
        kernel: Arc<VersionSetKernel>,
    ) -> Result<ManifestEngine> {
        let (tx, mut rx) = unbounded();
        let mut manifest = {
            let version_set = version_set_with_lock.lock().unwrap();
            let cfs = version_set.get_column_familys();
            let mut versions = HashMap::default();
            for cf in cfs {
                versions.insert(cf.get_id() as u32, cf.get_version());
            }
            ManifestWriter::new(
                versions,
                version_set_with_lock.clone(),
                kernel.clone(),
                options.clone(),
            )
        };
        pool.spawn(async move {
            while let Some(x) = rx.next().await {
                manifest.batch(x);
            }
        });
        let engine = ManifestEngine::new(tx, comparator);
        Ok(engine)
    }
}
