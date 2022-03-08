use crate::common::{Error, Result};
use crate::core::Core;
use crate::version::snapshot::Snapshot;
use crate::{ColumnFamilyDescriptor, DBIterator, DBOptions, ReadOptions, WriteBatch};
use std::cmp::Ordering;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use crossbeam::channel::unbounded;
use futures::channel::mpsc::UnboundedSender;
use yatp::{task::future::TaskCell, ThreadPool};
use crate::compaction::FlushRequest;

pub struct UpdateEngine {
    core: Core,
    last_sequence: u64,
}

#[derive(Clone)]
pub struct ReadonlyEngine {
    core: Core,
}

impl UpdateEngine {
    pub async fn open(
        db_options: DBOptions,
        cfs: Vec<ColumnFamilyDescriptor>,
        other_pool: &ThreadPool<TaskCell>,
    ) -> Result<Self> {
        let immutable_options = Arc::new(db_options.into());
        let core = Core::recover(immutable_options, &cfs, &pool).await?;
        let (flush_tx, flush_rx) = unbounded();
        let (compaction_tx, compaction_rx) = unbounded();
        let mut engine = UpdateEngine {
            last_sequence: core.ctx.last_sequence(),
            core,
        };
        engine.run_flush_job(flush_rx, compaction_tx)?;
        engine.run_compaction_job(compaction_rx)?;
        let mut created_cfs: HashSet<String, RandomState> = HashSet::default();
        {
            let mut vs = engine.core.version_set.lock().unwrap();
            for desc in &cfs {
                if vs.mut_column_family_by_name(&desc.name).is_some() {
                    created_cfs.insert(desc.name.clone());
                }
            }
        }
        for desc in cfs {
            if created_cfs.get(&desc.name).is_some() {
                continue;
            }
            engine
                .create_column_family(&desc.name, desc.options)
                .await?;
        }
        Ok(engine)
    }

    pub fn write(&mut self, wb: &mut WriteBatch) -> Result<()> {
        let mut rwb = wb.to_raw();
        rwb.set_sequence(self.last_sequence + 1);
        let commit_sequence = self.last_sequence + rwb.count() as u64;
        self.last_sequence = commit_sequence;
        self.core.ctx.set_last_sequence(commit_sequence);
        Ok(())
    }

    pub fn get_readonly_engine(&self) -> ReadonlyEngine {
        ReadonlyEngine {
            core: self.core.clone(),
        }
    }
}

impl ReadonlyEngine {
    pub async fn get(&self, opts: &ReadOptions, cf: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.core.get(opts, cf, key).await
    }

    pub fn get_snapshot(&self) -> Box<Snapshot> {
        self.core.get_snapshot()
    }

    pub fn release_snapshot(&self, snapshot: Box<Snapshot>) {
        self.core.release_snapshot(snapshot)
    }

    pub fn new_iterator(&self, opts: &ReadOptions, cf: u32) -> Result<DBIterator> {
        self.core.new_iterator(opts, cf)
    }
}
