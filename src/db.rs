use std::cell::RefCell;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::common::format::ValueType;
use crate::common::{
    make_current_file, make_log_file, parse_file_name, DBFileType, Error, Result,
    MAX_SEQUENCE_NUMBER,
};
use crate::compaction::{
    run_compaction_job, run_flush_memtable_job, FlushRequest, LevelCompactionPicker,
};
use crate::iterator::DBIterator;
use crate::log::LogReader;
use crate::manifest::{Manifest, ManifestScheduler, ManifestWriter};
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyDescriptor, DBOptions, ImmutableDBOptions, ReadOptions};
use crate::version::{KernelNumberContext, VersionSet};
use crate::wal::{WALScheduler, WALTask, WALWriter, WriteMemtableTask};
use crate::write_batch::{ReadOnlyWriteBatch, WriteBatch, WriteBatchItem};
use crate::ColumnFamilyOptions;

use crate::pipeline::PipelineCommitQueue;
use crate::version::snapshot::{Snapshot, SnapshotList};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot::Sender;
use futures::{SinkExt, StreamExt};
use yatp::{task::future::TaskCell, Builder as PoolBuilder, ThreadPool};

#[derive(Default)]
pub struct PerfContext {
    write_wal: u64,
    write_memtable: u64,
    other: u64,
}

thread_local! {
    pub static PERF_CTX: RefCell<PerfContext> = RefCell::new(PerfContext::default());
}

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
    snapshot_list: Arc<Mutex<SnapshotList>>,
    commit_queue: Arc<PipelineCommitQueue>,
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
            engine
                .create_column_family(&desc.name, desc.options)
                .await?;
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
        let step1 = time::precise_time_ns();
        let rwb = wb.to_raw();
        let rwb = self
            .wal_scheduler
            .schedule_writebatch(rwb, sync, disable_wal)
            .await?;
        let step2 = time::precise_time_ns();
        let sequence = rwb.wb.get_sequence();
        let last_commit_sequence = sequence - 1;
        let commit_sequence = sequence + rwb.wb.count() as u64 - 1;
        self.write_memtable(&rwb.wb, sequence, &rwb.mems)?;
        let step3 = time::precise_time_ns();
        self.commit_queue
            .commit(last_commit_sequence, commit_sequence);
        wb.recycle(rwb.wb);
        let step4 = time::precise_time_ns();
        PERF_CTX.with(|x| {
            let mut ctx = x.borrow_mut();
            ctx.write_wal += (step2 - step1) / 1000;
            ctx.write_memtable += (step3 - step2) / 1000;
            ctx.other += (step4 - step3) / 1000;
        });
        Ok(())
    }

    pub async fn create_column_family(
        &mut self,
        name: &str,
        options: ColumnFamilyOptions,
    ) -> Result<u32> {
        self.wal_scheduler
            .schedule_create_column_family(name, options)
            .await
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
        let sequence = self.kernel.last_sequence();
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
        let sequence = opts.snapshot.clone().unwrap_or(self.kernel.last_sequence());
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
        let (tx, rx) = unbounded();
        let (flush_tx, flush_rx) = unbounded();
        let (compaction_tx, compaction_rx) = unbounded();
        let manifest_scheduler = Self::start_manifest_job(&pool, Box::new(manifest))?;
        let wal_scheduler = WALScheduler::new(tx);
        let mut engine = Engine {
            version_set,
            commit_queue: Arc::new(PipelineCommitQueue::new(kernel.clone())),
            kernel,
            pool,
            options: immutable_options,
            flush_scheduler: flush_tx.clone(),
            manifest_scheduler,
            wal_scheduler,
            stopped: Arc::new(AtomicBool::new(false)),
            snapshot_list: Arc::new(Mutex::new(SnapshotList::new())),
        };
        engine.recover_log(logs).await?;
        engine.run_wal_job(flush_tx, rx)?;
        engine.run_flush_job(flush_rx, compaction_tx)?;
        engine.run_compaction_job(compaction_rx)?;
        Ok(engine)
    }

    async fn recover_log(&mut self, logs: Vec<u64>) -> Result<()> {
        let mut min_log_number = u64::MAX;
        let (versions, cf_mems) = {
            let version_set = self.version_set.lock().unwrap();
            (
                version_set.get_column_family_versions(),
                version_set.get_column_family_memtables(),
            )
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
                self.write_memtable(&wb, sequence, &cf_mems)?;
                next_seq = sequence + count - 1;
                // TODO: flush if the memtable is full
            }
            self.kernel.set_last_sequence(next_seq);
        }
        Ok(())
    }

    fn write_memtable(
        &mut self,
        wb: &ReadOnlyWriteBatch,
        mut sequence: u64,
        cf_mems: &[(u32, Arc<Memtable>)],
    ) -> Result<()> {
        pub fn check_memtable_cf(mems: &[(u32, Arc<Memtable>)], cf: u32) -> usize {
            let mut idx = cf as usize;
            if idx >= mems.len() || cf != mems[idx].0 {
                idx = mems.len();
                for i in 0..mems.len() {
                    if mems[i].0 == cf {
                        idx = i;
                        break;
                    }
                }
                if idx == mems.len() {
                    panic!("write miss column family, {}, mem size: {}", cf, mems.len());
                }
            }
            idx
        }
        for item in wb.iter() {
            match item {
                WriteBatchItem::Put { cf, key, value } => {
                    let idx = check_memtable_cf(cf_mems, cf);
                    cf_mems[idx]
                        .1
                        .add(key, value, sequence, ValueType::TypeValue);
                }
                WriteBatchItem::Delete { cf, key } => {
                    let idx = check_memtable_cf(cf_mems, cf);
                    cf_mems[idx].1.delete(key, sequence);
                }
            }
            sequence += 1;
        }
        Ok(())
    }

    fn run_compaction_job(&self, mut rx: UnboundedReceiver<u32>) -> Result<()> {
        let cf_opts = {
            let vs = self.version_set.lock().unwrap();
            vs.get_column_family_options()
        };
        let picker = LevelCompactionPicker::new(cf_opts, self.options.clone());
        let remote = self.pool.remote().clone();
        let version_set = self.version_set.clone();
        let manifest_scheduler = self.manifest_scheduler.clone();
        let kernel = self.kernel.clone();
        self.pool.spawn(async move {
            while let Some(cf) = rx.next().await {
                let version = {
                    let vs = version_set.lock().unwrap();
                    let super_version = vs.get_superversion(cf).unwrap();
                    super_version.current.clone()
                };
                if let Some(request) = picker.pick_compaction(cf, version) {
                    let engine = manifest_scheduler.clone();
                    let kernel = kernel.clone();
                    remote.spawn(async move {
                        let _ = run_compaction_job(engine, request, kernel).await;
                    });
                }
            }
        });
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

    fn run_wal_job(
        &self,
        flush_scheduler: UnboundedSender<FlushRequest>,
        mut rx: UnboundedReceiver<WALTask>,
    ) -> Result<()> {
        let writer = WALWriter::new(
            self.kernel.clone(),
            self.version_set.clone(),
            self.options.clone(),
            flush_scheduler,
            self.manifest_scheduler.clone(),
        )?;
        let mut processor = BatchWALProcessor::new(writer);

        let f = async move {
            while let Some(x) = rx.next().await {
                match x {
                    WALTask::Write {
                        wb,
                        cb,
                        sync,
                        disable_wal,
                    } => {
                        processor.writer.preprocess_write().await?;
                        processor.batch(wb, cb, disable_wal, sync);
                    }
                    WALTask::Ingest { .. } => {
                        unimplemented!();
                    }
                    WALTask::CreateColumnFamily { name, opts, cb } => {
                        processor.flush().await?;
                        let ret = processor.writer.create_column_family(name, opts).await;
                        cb.send(ret).unwrap();
                    }
                }
                while let Ok(x) = rx.try_next() {
                    match x {
                        Some(WALTask::Write {
                            wb,
                            cb,
                            sync,
                            disable_wal,
                        }) => {
                            processor.batch(wb, cb, disable_wal, sync);
                            if processor.should_flush() {
                                break;
                            }
                        }
                        Some(WALTask::CreateColumnFamily { name, opts, cb }) => {
                            processor.flush().await?;
                            let ret = processor.writer.create_column_family(name, opts).await;
                            cb.send(ret).unwrap();
                        }
                        Some(WALTask::Ingest { .. }) => {
                            unimplemented!();
                        }
                        None => {
                            processor.flush().await?;
                            return Ok(());
                        }
                    }
                }
                processor.flush().await?;
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

    fn run_flush_job(
        &self,
        mut rx: UnboundedReceiver<FlushRequest>,
        mut compaction_scheduler: UnboundedSender<u32>,
    ) -> Result<()> {
        let engine = self.manifest_scheduler.clone();
        let kernel = self.kernel.clone();
        let options = self.options.clone();
        let stopped = self.stopped.clone();
        let version_set = self.version_set.clone();
        let snapshot_list = self.snapshot_list.clone();
        let cf_options = version_set.lock().unwrap().get_column_family_options();
        let commit_queue = self.commit_queue.clone();

        let f = async move {
            while let Some(req) = rx.next().await {
                // We must wait other thread finish their write task to the current memtables.
                commit_queue.wait_pending_writers(req.wait_commit_request);
                let mut snapshots = vec![];
                {
                    let mut snapshot_guard = snapshot_list.lock().unwrap();
                    snapshot_guard.collect_snapshots(&mut snapshots);
                }
                let cfs: Vec<u32> = req.mems.iter().map(|(cf, _)| *cf).collect();
                run_flush_memtable_job(
                    engine.clone(),
                    vec![req],
                    kernel.clone(),
                    options.clone(),
                    cf_options.clone(),
                    snapshots,
                )
                .await?;
                for cf in cfs {
                    let _ = compaction_scheduler.send(cf).await;
                }
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

    pub fn close(&mut self) -> Result<()> {
        self.stopped.store(true, Ordering::Release);
        self.pool.shutdown();
        Ok(())
    }
}

const MAX_BATCH_SIZE: usize = 1 << 20; // 1MB

pub struct BatchWALProcessor {
    writer: WALWriter,
    tasks: Vec<(ReadOnlyWriteBatch, Sender<Result<WriteMemtableTask>>, bool)>,
    need_sync: bool,
    batch_size: usize,
}

impl BatchWALProcessor {
    pub fn new(writer: WALWriter) -> Self {
        Self {
            writer,
            tasks: vec![],
            need_sync: false,
            batch_size: 0,
        }
    }

    pub fn batch(
        &mut self,
        wb: ReadOnlyWriteBatch,
        cb: Sender<Result<WriteMemtableTask>>,
        disable_wal: bool,
        sync: bool,
    ) {
        if sync {
            self.need_sync = true;
        }
        if !disable_wal {
            self.batch_size += wb.get_data().len();
        }
        self.tasks.push((wb, cb, disable_wal));
    }

    pub fn should_flush(&self) -> bool {
        self.batch_size > MAX_BATCH_SIZE
    }

    pub async fn flush(&mut self) -> Result<()> {
        let r = self.writer.write(&mut self.tasks, self.need_sync).await;
        self.need_sync = false;
        self.batch_size = 0;
        for (wb, cb, _) in self.tasks.drain(..) {
            match &r {
                Err(e) => {
                    let _ = cb.send(Err(e.clone()));
                }
                Ok(mems) => {
                    let _ = cb.send(Ok(WriteMemtableTask {
                        wb,
                        mems: mems.clone(),
                    }));
                }
            }
        }
        r.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::AsyncFileSystem;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn build_small_write_buffer_db(dir: TempDir) -> Engine {
        let r = Runtime::new().unwrap();
        let mut db_options = DBOptions::default();
        db_options.fs = Arc::new(AsyncFileSystem::new(2));
        db_options.db_path = dir.path().to_str().unwrap().to_string();
        let mut cf_opt = ColumnFamilyOptions::default();
        cf_opt.write_buffer_size = 128 * 1024;
        let cfs = vec![ColumnFamilyDescriptor {
            name: "default".to_string(),
            options: cf_opt,
        }];
        let engine = r
            .block_on(Engine::open(db_options.clone(), cfs.clone(), None))
            .unwrap();
        engine
    }

    #[test]
    fn test_open_new_db_and_reopen() {
        let dir = tempfile::Builder::new()
            .prefix("test_open_new_db")
            .tempdir()
            .unwrap();
        let r = Runtime::new().unwrap();
        let mut db_options = DBOptions::default();
        db_options.fs = Arc::new(AsyncFileSystem::new(2));
        db_options.db_path = dir.path().to_str().unwrap().to_string();
        let mut engine = r
            .block_on(Engine::open(db_options.clone(), vec![], None))
            .unwrap();
        let vs = engine.version_set.lock().unwrap();
        assert_eq!(vs.get_column_family_versions().len(), 1);
        drop(vs);
        engine.close().unwrap();
        drop(engine);
        let mut write_opt = ColumnFamilyOptions::default();
        write_opt.write_buffer_size = 1000;
        let cfs = vec![ColumnFamilyDescriptor {
            name: "write".to_string(),
            options: write_opt,
        }];
        let mut engine = r
            .block_on(Engine::open(db_options.clone(), cfs.clone(), None))
            .unwrap();
        let vs = engine.version_set.lock().unwrap();
        assert_eq!(vs.get_column_family_versions().len(), 2);
        drop(vs);
        let mut wb = WriteBatch::new();
        wb.put_cf(0, b"k1", b"v1");
        r.block_on(engine.write(&mut wb)).unwrap();
        let opts = ReadOptions::default();
        let v = r.block_on(engine.get(&opts, 0, b"k1")).unwrap().unwrap();
        assert_eq!(v, b"v1".to_vec());
        engine.close().unwrap();
        drop(engine);
        let engine = r.block_on(Engine::open(db_options, cfs, None)).unwrap();
        let v = r.block_on(engine.get(&opts, 0, b"k1")).unwrap().unwrap();
        assert_eq!(v, b"v1".to_vec());
    }

    #[test]
    fn test_switch_memtable_and_flush() {
        let dir = tempfile::Builder::new()
            .prefix("test_switch_memtable_and_flush")
            .tempdir()
            .unwrap();
        let mut engine = build_small_write_buffer_db(dir);
        let r = Runtime::new().unwrap();
        let mut bench_ret = vec![0; 10000];
        let mut wb = WriteBatch::new();
        const TOTAL_CASES: usize = 100;
        let mut prev_perf = 0;
        let total_time = Instant::now();
        for i in 0..TOTAL_CASES {
            for j in 0..100 {
                let k = (i * 100 + j).to_string();
                wb.put_cf(0, k.as_bytes(), b"v00000000000001");
            }
            r.block_on(engine.write_opt(&mut wb, false, false)).unwrap();
            wb.clear();
            let now = PERF_CTX.with(|ctx| ctx.borrow().write_wal);
            let c = now - prev_perf;
            prev_perf = now;
            if c / 100 >= 10000 {
                println!("tail latency: {:?}ns", c);
            } else {
                bench_ret[(c / 100) as usize] += 1;
            }
        }
        println!("total cost time: {:?}", total_time.elapsed());
        for i in 0..10000 {
            if bench_ret[i] > 0 {
                let f = (bench_ret[i] * 100) as f64 / TOTAL_CASES as f64;
                println!("{}% latency is between [{},{})", f, i * 100, (i + 1) * 100);
            }
        }
        sleep(Duration::from_secs(2));
        let vs = engine.version_set.lock().unwrap();
        let v = vs.get_superversion(0).unwrap();
        assert_eq!(v.current.get_storage_info().get_level0_file_num(), 6);
    }

    #[test]
    fn test_snapshot_and_iterator() {
        let dir = tempfile::Builder::new()
            .prefix("test_snapshot_and_iterator")
            .tempdir()
            .unwrap();
        let mut engine = build_small_write_buffer_db(dir);
        let r = Runtime::new().unwrap();
        let mut wb = WriteBatch::new();
        for j in 0..100 {
            let k = (100 + j).to_string();
            wb.put_cf(0, k.as_bytes(), b"v00000000000001");
        }
        r.block_on(engine.write_opt(&mut wb, false, false)).unwrap();
        wb.clear();
        let snapshot = engine.get_snapshot();
        for j in 100..200 {
            let k = (100 + j).to_string();
            wb.put_cf(0, k.as_bytes(), b"v00000000000001");
        }
        let mut opts = ReadOptions::default();
        opts.snapshot = Some(snapshot.get_sequence());
        let mut iter = engine.new_iterator(&opts, 0).unwrap();
        r.block_on(iter.seek_to_first());
        for j in 0..100 {
            let k = (100 + j).to_string();
            assert_eq!(k.as_bytes(), iter.key());
            assert_eq!(b"v00000000000001", iter.value());
            r.block_on(iter.next());
        }
    }
}
