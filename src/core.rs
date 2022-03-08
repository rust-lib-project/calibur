use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::common::{make_current_file, Error, Result, MAX_SEQUENCE_NUMBER};
use crate::iterator::DBIterator;
use crate::manifest::{Manifest, ManifestScheduler, ManifestWriter};
use crate::options::{ColumnFamilyDescriptor, ImmutableDBOptions, ReadOptions};
use crate::version::{KernelNumberContext, VersionSet};

use crate::version::snapshot::{Snapshot, SnapshotList};
use futures::channel::mpsc::unbounded;
use futures::StreamExt;
use yatp::{task::future::TaskCell, ThreadPool};

#[derive(Clone)]
pub struct Core {
    pub version_set: Arc<Mutex<VersionSet>>,
    pub ctx: Arc<KernelNumberContext>,
    pub options: Arc<ImmutableDBOptions>,
    pub stopped: Arc<AtomicBool>,
    pub snapshot_list: Arc<Mutex<SnapshotList>>,
    pub manifest_scheduler: ManifestScheduler,
}

impl Core {
    pub async fn recover(
        immutable_options: Arc<ImmutableDBOptions>,
        cfs: &[ColumnFamilyDescriptor],
        pool: &ThreadPool<TaskCell>,
    ) -> Result<Self> {
        let current = make_current_file(&immutable_options.db_path);
        let manifest = if !immutable_options.fs.file_exist(&current)? {
            Manifest::create(cfs, &immutable_options).await?
        } else {
            Manifest::recover(cfs, &immutable_options).await?
        };
        let version_set = manifest.get_version_set();
        let kernel = version_set.lock().unwrap().get_kernel();
        let manifest_scheduler = Self::start_manifest_job(pool, Box::new(manifest))?;
        Ok(Core {
            version_set,
            ctx: kernel,
            options: immutable_options,
            stopped: Arc::new(AtomicBool::new(false)),
            snapshot_list: Arc::new(Mutex::new(SnapshotList::new())),
            manifest_scheduler,
        })
    }

    pub async fn get(&self, opts: &ReadOptions, cf: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let version = {
            let vs = self.version_set.lock().unwrap();
            match vs.get_superversion(cf) {
                Some(v) => v,
                None => return Err(Error::InvalidColumnFamily(cf)),
            }
        };
        version.get(opts, key, MAX_SEQUENCE_NUMBER).await
    }

    pub fn get_snapshot(&self) -> Box<Snapshot> {
        let mut snapshot_list = self.snapshot_list.lock().unwrap();
        let sequence = self.ctx.last_sequence();
        snapshot_list.new_snapshot(sequence)
    }

    pub fn release_snapshot(&self, snapshot: Box<Snapshot>) {
        let mut snapshot_list = self.snapshot_list.lock().unwrap();
        snapshot_list.release_snapshot(snapshot)
    }

    pub fn new_iterator(&self, opts: &ReadOptions, cf: u32) -> Result<DBIterator> {
        let version = {
            let vs = self.version_set.lock().unwrap();
            match vs.get_superversion(cf) {
                Some(v) => v,
                None => return Err(Error::InvalidColumnFamily(cf)),
            }
        };
        let iter = version.new_iterator(opts)?;
        let sequence = opts.snapshot.clone().unwrap_or(self.ctx.last_sequence());
        Ok(DBIterator::new(
            iter,
            version
                .column_family_options
                .comparator
                .get_user_comparator()
                .clone(),
            sequence,
        ))
    }

    fn start_manifest_job(
        pool: &ThreadPool<TaskCell>,
        manifest: Box<Manifest>,
    ) -> Result<ManifestScheduler> {
        let (tx, mut rx) = unbounded();
        let mut writer = ManifestWriter::new(manifest);
        pool.spawn(async move {
            while let Some(x) = rx.next().await {
                writer.batch(x);
                while let Ok(x) = rx.try_next() {
                    match x {
                        Some(msg) => {
                            if writer.batch(msg) {
                                break;
                            }
                        }
                        None => {
                            writer.apply().await;
                            return;
                        }
                    }
                }
                writer.apply().await;
            }
        });
        let engine = ManifestScheduler::new(tx);
        Ok(engine)
    }
}
