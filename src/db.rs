use crate::common::{make_current_file, make_log_file, parse_file_name, DBFileType, Error, Result};
use crate::options::{ColumnFamilyDescriptor, DBOptions, ImmutableDBOptions};
use crate::version::{KernelNumberContext, VersionSet};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::RandomState;
use std::path::PathBuf;

use crate::common::format::ValueType;
use crate::compaction::{run_flush_memtable_job, FlushRequest};
use crate::log::LogReader;
use crate::manifest::{Manifest, ManifestScheduler, ManifestWriter};
use crate::memtable::Memtable;
use crate::wal::{WALScheduler, WALTask, WALWriter};
use crate::write_batch::{ReadOnlyWriteBatch, WriteBatch, WriteBatchItem};
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use yatp::{task::future::TaskCell, Builder as PoolBuilder, ThreadPool};
use crate::ColumnFamilyOptions;

#[derive(Clone)]
pub struct Engine {
    version_set: Arc<Mutex<VersionSet>>,
    kernel: Arc<KernelNumberContext>,
    pool: Arc<ThreadPool<TaskCell>>,
    options: Arc<ImmutableDBOptions>,
    manifest_scheduler: ManifestScheduler,
    wal_scheduler: WALScheduler,
    stopped: Arc<AtomicBool>,
    flush_scheduler: UnboundedSender<FlushRequest>,
}

impl Engine {
    pub async fn open(
        db_options: DBOptions,
        cfs: Vec<ColumnFamilyDescriptor>,
        other_pool: Option<Arc<ThreadPool<TaskCell>>>,
    ) -> Result<Self> {
        let immutable_options = Arc::new(db_options.into());
        let mut engine = Self::recover(immutable_options, &cfs, other_pool).await?;
        let mut created_cfs: HashSet<String, RandomState> = HashSet::default();
        {
            let mut vs = engine.version_set.lock().unwrap();
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
            engine.create_column_family(&desc.name, desc.options).await?;
        }
        Ok(engine)
    }

    pub async fn write(&mut self, wb: &mut WriteBatch) -> Result<()> {
        self.write_opt(wb, false, false).await
    }

    pub async fn write_opt(
        &mut self,
        wb: &mut WriteBatch,
        disable_wal: bool,
        sync: bool,
    ) -> Result<()> {
        let rwb = wb.to_raw();
        let rwb = self.wal_scheduler.send(rwb, sync, disable_wal).await?;
        let sequence = rwb.wb.get_sequence();
        for item in rwb.wb.iter() {
            match item {
                WriteBatchItem::Put { cf, key, value } => {
                    let idx = rwb.check_memtable_cf(cf);
                    rwb.mems[idx]
                        .1
                        .add(key, value, sequence, ValueType::TypeValue);
                }
                WriteBatchItem::Delete { cf, key } => {
                    let idx = rwb.check_memtable_cf(cf);
                    rwb.mems[idx].1.delete(key, sequence);
                }
            }
        }

        // TODO: check atomic flush.
        for (cf, m) in rwb.mems {
            if m.mark_write_done() {
                self.schedule_flush(cf, m).await;
            }
        }

        wb.recycle(rwb.wb);
        Ok(())
    }

    pub async fn create_column_family(&mut self, name: &str, options: ColumnFamilyOptions) -> Result<u32> {
        Ok(0)
    }

    async fn recover(
        immutable_options: Arc<ImmutableDBOptions>,
        cfs: &[ColumnFamilyDescriptor],
        other_pool: Option<Arc<ThreadPool<TaskCell>>>,
    ) -> Result<Self> {
        let current = make_current_file(&immutable_options.db_path);
        let manifest = if !immutable_options.fs.file_exist(&current)? {
            Manifest::create(cfs, &immutable_options).await?
        } else {
            Manifest::recover(cfs, &immutable_options).await?
        };
        let version_set = manifest.get_version_set();
        let files = immutable_options
            .fs
            .list_files(PathBuf::from(immutable_options.db_path.clone()))?;
        let mut logs = vec![];
        for f in files {
            let fname = f
                .file_name()
                .unwrap()
                .to_str()
                .ok_or(Error::InvalidFile(format!(
                    "file {:?} can not convert to string",
                    f
                )))?;
            let (db_tp, file_number) = parse_file_name(fname)?;
            if db_tp == DBFileType::LogFile {
                logs.push(file_number);
            }
        }
        let kernel = version_set.lock().unwrap().get_kernel();
        let pool = other_pool.unwrap_or_else(|| {
            let mut builder = PoolBuilder::new("rocksdb");
            let pool = builder
                .max_thread_count(immutable_options.max_background_jobs)
                .build_multilevel_future_pool();
            Arc::new(pool)
        });
        let manifest_scheduler = Self::start_manifest_job(&pool, Box::new(manifest))?;
        let (tx, rx) = unbounded();
        let (flush_tx, flush_rx) = unbounded();
        let wal_scheduler = WALScheduler::new(tx);
        let mut engine = Engine {
            version_set,
            kernel,
            pool,
            options: immutable_options,
            flush_scheduler: flush_tx.clone(),
            manifest_scheduler,
            wal_scheduler,
            stopped: Arc::new(AtomicBool::new(false)),
        };
        engine.recover_log(logs).await?;
        engine.run_wal_job(flush_tx, rx)?;
        engine.run_flush_job(flush_rx)?;
        Ok(engine)
    }

    async fn recover_log(&mut self, logs: Vec<u64>) -> Result<()> {
        let mut min_log_number = u64::MAX;
        let versions = {
            let version_set = self.version_set.lock().unwrap();
            version_set.get_column_family_versions()
        };
        for v in &versions {
            min_log_number = std::cmp::min(min_log_number, v.get_log_number());
        }
        for log_number in logs {
            if log_number < min_log_number {
                continue;
            }
            self.kernel.mark_file_number_used(log_number);
            let fname = make_log_file(&self.options.db_path, log_number);
            let reader = self.options.fs.open_sequencial_file(fname)?;
            let mut log_reader = LogReader::new(reader);
            let mut buf = vec![];
            let mut next_seq = self.kernel.last_sequence();
            while log_reader.read_record(&mut buf).await? {
                let wb = ReadOnlyWriteBatch::try_from(buf.clone())?;
                let count = wb.count() as u64;
                let sequence = wb.get_sequence();
                self.write_memtable(&wb, sequence)?;
                next_seq = sequence + count - 1;
                // TODO: flush if the memtable is full
            }
            self.kernel.set_last_sequence(next_seq);
        }
        Ok(())
    }

    fn seek_to_mem<F: FnMut(&Arc<Memtable>)>(
        &mut self,
        mems: &mut Vec<Option<Arc<Memtable>>>,
        cf: u32,
        mut f: F,
    ) -> Result<()> {
        let idx = cf as usize;
        if mems.len() <= idx {
            while mems.len() <= idx {
                mems.push(None);
            }
        }
        if mems[idx].is_none() {
            let vs = self.version_set.lock().unwrap();
            if let Some(mem) = vs.get_superversion(cf).map(|v| v.mem.clone()) {
                mems.push(Some(mem));
            } else {
                return Err(Error::Other(format!("Column family {} not exist", cf)));
            }
        }
        f(mems[idx].as_ref().unwrap());
        Ok(())
    }

    fn write_memtable(&mut self, wb: &ReadOnlyWriteBatch, sequence: u64) -> Result<()> {
        let mut cf_mems = vec![];
        for item in wb.iter() {
            match item {
                WriteBatchItem::Put { cf, key, value } => {
                    self.seek_to_mem(&mut cf_mems, cf, |mem| {
                        mem.add(key, value, sequence, ValueType::TypeValue);
                    })?;
                    // cf_mems[idx].as_ref().unwrap().add(key, value, sequence, ValueType::TypeValue);
                }
                WriteBatchItem::Delete { cf, key } => {
                    self.seek_to_mem(&mut cf_mems, cf, |mem| {
                        mem.delete(key, sequence);
                    })?;
                }
            }
        }
        Ok(())
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
                while let Some(x) = rx.try_next().unwrap() {
                    if writer.batch(x) {
                        break;
                    }
                }
                writer.apply().await;
            }
        });
        let engine = ManifestScheduler::new(tx);
        Ok(engine)
    }

    fn run_wal_job(
        &self,
        flush_scheduler: UnboundedSender<FlushRequest>,
        mut rx: UnboundedReceiver<WALTask>,
    ) -> Result<()> {
        let mut writer = WALWriter::new(
            self.kernel.clone(),
            self.version_set.clone(),
            self.options.clone(),
            flush_scheduler,
        )?;
        let f = async move {
            let mut tasks = vec![];
            while let Some(x) = rx.next().await {
                match x {
                    WALTask::Write {
                        wb,
                        cb,
                        sync,
                        disable_wal,
                    } => {
                        writer.preprocess_write().await?;
                        let task = writer.batch(wb, disable_wal, sync);
                        tasks.push((task, cb));
                    }
                    WALTask::Ingest { .. } => {
                        unimplemented!();
                    }
                }
                while let Some(x) = rx.try_next().unwrap() {
                    match x {
                        WALTask::Write {
                            wb,
                            cb,
                            sync,
                            disable_wal,
                        } => {
                            let task = writer.batch(wb, disable_wal, sync);
                            tasks.push((task, cb));
                            if writer.should_flush() {
                                break;
                            }
                        }
                        WALTask::Ingest { .. } => {
                            unimplemented!();
                        }
                    }
                }
                let mut r = writer.flush().await;
                if r.is_ok() {
                    r = writer.fsync().await;
                }
                match r {
                    Ok(()) => {
                        for (wb, cb) in tasks.drain(..) {
                            let _ = cb.send(Ok(wb));
                        }
                    }
                    Err(e) => {
                        for (_, cb) in tasks.drain(..) {
                            let _ = cb.send(Err(e.clone()));
                        }
                        return Err(e);
                    }
                }
            }
            Ok(())
        };
        let version_set = self.version_set.clone();
        let stopped = self.stopped.clone();
        self.pool.spawn(async move {
            if let Err(e) = f.await {
                if !stopped.load(Ordering::Acquire) {
                    let mut v = version_set.lock().unwrap();
                    v.record_error(e);
                }
            }
        });
        Ok(())
    }

    fn run_flush_job(&self, mut rx: UnboundedReceiver<FlushRequest>) -> Result<()> {
        let engine = self.manifest_scheduler.clone();
        let kernel = self.kernel.clone();
        let options = self.options.clone();
        let stopped = self.stopped.clone();
        let version_set = self.version_set.clone();
        let mut cf_options = HashMap::default();
        for (cf, opt) in version_set.lock().unwrap().get_column_family_options() {
            cf_options.insert(cf, opt);
        }

        let f = async move {
            while let Some(x) = rx.next().await {
                run_flush_memtable_job(
                    engine.clone(),
                    vec![x],
                    kernel.clone(),
                    options.clone(),
                    cf_options.clone(),
                )
                .await?;
            }
            Ok(())
        };
        self.pool.spawn(async move {
            if let Err(e) = f.await {
                if !stopped.load(Ordering::Acquire) {
                    let mut v = version_set.lock().unwrap();
                    v.record_error(e);
                }
            }
        });

        Ok(())
    }

    async fn schedule_flush(&mut self, cf: u32, mem: Arc<Memtable>) {
        self.flush_scheduler
            .send(FlushRequest::new(cf, mem))
            .await
            .unwrap_or_else(|_| {
                if !self.stopped.load(Ordering::Acquire) {
                    let mut v = self.version_set.lock().unwrap();
                    v.record_error(Error::Other(format!("schedule flush failed")));
                }
            });
    }
}
