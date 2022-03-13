use std::cell::RefCell;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::path::Path;
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
use crate::manifest::{start_manifest_job, Manifest, ManifestScheduler};
use crate::options::{ColumnFamilyDescriptor, DBOptions, ImmutableDBOptions, ReadOptions};
use crate::version::{KernelNumberContext, SuperVersion, VersionSet};
use crate::wal::{WALScheduler, WALTask, WALWriter, WriteMemtableTask};
use crate::write_batch::{ReadOnlyWriteBatch, WriteBatch, WriteBatchItem};
use crate::ColumnFamilyOptions;

use crate::memtable::Memtable;
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
        self.write_memtable(&rwb.wb, sequence, &rwb.cfs)?;
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
        let sequence = opts.snapshot.unwrap_or_else(|| self.kernel.last_sequence());
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
            .list_files(Path::new(immutable_options.db_path.as_str()))?;
        let mut logs = vec![];
        for f in files {
            let fname = f.file_name().unwrap().to_str().ok_or_else(|| {
                Error::InvalidFile(format!("file {:?} can not convert to string", f))
            })?;
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
                .stack_size(8 * 1024 * 1024)
                .build_multilevel_future_pool();
            Arc::new(pool)
        });
        let (tx, rx) = unbounded();
        let (flush_tx, flush_rx) = unbounded();
        let (compaction_tx, compaction_rx) = unbounded();
        let manifest_scheduler = start_manifest_job(&pool, Box::new(manifest))?;
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
        pub fn check_memtable_cf(cfs: &[Arc<SuperVersion>], cf: u32) -> usize {
            let mut idx = cf as usize;
            if idx >= cfs.len() || cf != cfs[idx].id {
                idx = cfs.len();
                for i in 0..cfs.len() {
                    if cfs[i].id == cf {
                        idx = i;
                        break;
                    }
                }
                if idx == cfs.len() {
                    panic!("write miss column family, {}, cf size: {}", cf, cfs.len());
                }
            }
            idx
        }

        let mut min_log_number = u64::MAX;
        let (versions, column_families) = {
            let version_set = self.version_set.lock().unwrap();
            (
                version_set.get_column_family_versions(),
                version_set.get_column_family_superversion(),
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
            let reader = self.options.fs.open_sequential_file(&fname)?;
            let mut log_reader = LogReader::new(reader);
            let mut buf = vec![];
            let mut next_seq = self.kernel.last_sequence();
            while log_reader.read_record(&mut buf).await? {
                let wb = ReadOnlyWriteBatch::try_from(buf.clone())?;
                let count = wb.count() as u64;
                let mut sequence = wb.get_sequence();
                for item in wb.iter() {
                    match item {
                        WriteBatchItem::Put { cf, key, value } => {
                            let idx = check_memtable_cf(&column_families, cf);
                            if log_number > 0
                                && column_families[idx].current.get_log_number() > log_number
                            {
                                continue;
                            }
                            column_families[idx].mem.add(
                                key,
                                value,
                                sequence,
                                ValueType::TypeValue,
                            );
                        }
                        WriteBatchItem::Delete { cf, key } => {
                            let idx = check_memtable_cf(&column_families, cf);
                            if log_number > 0
                                && column_families[idx].current.get_log_number() > log_number
                            {
                                continue;
                            }
                            column_families[idx].mem.delete(key, sequence);
                        }
                    }
                    sequence += 1;
                }
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
        cf_mems: &[Arc<Memtable>],
    ) -> Result<()> {
        pub fn check_memtable_cf(mems: &[Arc<Memtable>], cf: u32) -> usize {
            let mut idx = cf as usize;
            if idx >= mems.len() || cf != mems[idx].get_column_family_id() {
                idx = mems.len();
                for (i, memtables) in mems.iter().enumerate() {
                    if memtables.get_column_family_id() == cf {
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
                    cf_mems[idx].add(key, value, sequence, ValueType::TypeValue);
                }
                WriteBatchItem::Delete { cf, key } => {
                    let idx = check_memtable_cf(cf_mems, cf);
                    cf_mems[idx].delete(key, sequence);
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
                        if disable_wal {
                            processor.write(wb, cb);
                        } else {
                            processor.batch(wb, cb, sync);
                        }
                    }
                    WALTask::Ingest { .. } => {
                        unimplemented!();
                    }
                    WALTask::CreateColumnFamily { name, opts, cb } => {
                        let ret = processor.writer.create_column_family(name, opts).await;
                        cb.send(ret).unwrap();
                    }
                    WALTask::CompactLog { cf } => {
                        processor.writer.compact_log(cf)?;
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
                            if disable_wal {
                                processor.flush().await?;
                                processor.write(wb, cb);
                            } else {
                                processor.batch(wb, cb, sync);
                                if processor.should_flush() {
                                    break;
                                }
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
                        Some(WALTask::CompactLog { cf }) => {
                            if processor.tasks.len() > 0 {
                                processor.flush().await?;
                            }
                            processor.writer.compact_log(cf)?;
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
        let mut wal_scheduler = self.wal_scheduler.clone();

        let f = async move {
            while let Some(req) = rx.next().await {
                // We must wait other thread finish their write task to the current memtables.
                if commit_queue.wait_pending_writers(req.wait_commit_request) {
                    return Ok(());
                }
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
                    let _ = wal_scheduler.schedule_compact_log(cf).await;
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
    tasks: Vec<(ReadOnlyWriteBatch, Sender<Result<WriteMemtableTask>>)>,
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
        sync: bool,
    ) {
        if sync {
            self.need_sync = true;
        }
        self.batch_size += wb.get_data().len();
        self.tasks.push((wb, cb));
    }

    pub fn should_flush(&self) -> bool {
        self.batch_size > MAX_BATCH_SIZE
    }

    pub fn write(&mut self, mut wb: ReadOnlyWriteBatch, cb: Sender<Result<WriteMemtableTask>>) {
        let cfs = self.writer.assign_sequence(&mut wb);
        let _ = cb.send(Ok(WriteMemtableTask { wb, cfs }));
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.tasks.is_empty() {
            return Ok(());
        }
        let r = self.writer.write(&mut self.tasks, self.need_sync).await;
        self.need_sync = false;
        self.batch_size = 0;
        for (wb, cb) in self.tasks.drain(..) {
            match &r {
                Err(e) => {
                    let _ = cb.send(Err(e.clone()));
                }
                Ok(mems) => {
                    let _ = cb.send(Ok(WriteMemtableTask {
                        wb,
                        cfs: mems.clone(),
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
    use crate::util;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn build_small_write_buffer_db(dir: &TempDir) -> Engine {
        let mut cf_opt = ColumnFamilyOptions::default();
        cf_opt.write_buffer_size = 128 * 1024;
        let cfs = vec![ColumnFamilyDescriptor {
            name: "default".to_string(),
            options: cf_opt,
        }];
        build_db(dir, cfs)
    }

    fn build_db(dir: &TempDir, cfs: Vec<ColumnFamilyDescriptor>) -> Engine {
        let r = Runtime::new().unwrap();
        let mut db_options = DBOptions::default();
        db_options.max_manifest_file_size = 128 * 1024;
        db_options.max_total_wal_size = 128 * 1024;
        db_options.fs = Arc::new(AsyncFileSystem::new(2));
        db_options.db_path = dir.path().to_str().unwrap().to_string();
        let engine = r
            .block_on(Engine::open(db_options.clone(), cfs, None))
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
        let mut engine = build_small_write_buffer_db(&dir);
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
        {
            let fs = engine.options.fs.clone();
            let vs = engine.version_set.lock().unwrap();
            let v = vs.get_superversion(0).unwrap();
            assert_eq!(v.current.get_storage_info().get_level0_file_num(), 2);
            assert_eq!(
                v.current
                    .get_storage_info()
                    .get_base_level_info()
                    .last()
                    .unwrap()
                    .tables
                    .size(),
                1
            );
            let files = fs.list_files(dir.path().to_path_buf()).unwrap();
            let mut count = 0;
            for f in files {
                if f.to_str().unwrap().ends_with("log") {
                    count += 1;
                }
            }
            assert_eq!(count, 1);
        }
        let opts = ReadOptions::default();
        let expected_value = b"v00000000000001".to_vec();
        for i in 0..TOTAL_CASES {
            for j in 0..100 {
                let k = (i * 100 + j).to_string();
                let v = r.block_on(engine.get(&opts, 0, k.as_bytes())).unwrap();
                assert!(v.is_some());
                assert_eq!(v.unwrap(), expected_value);
            }
        }
    }

    #[test]
    fn test_snapshot_and_iterator() {
        let dir = tempfile::Builder::new()
            .prefix("test_snapshot_and_iterator")
            .tempdir()
            .unwrap();
        let mut engine = build_small_write_buffer_db(&dir);
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
        assert!(!iter.valid());
    }

    #[test]
    fn test_switch_memtable_and_wal() {
        let dir = tempfile::Builder::new()
            .prefix("test_switch_memtable_and_wal")
            .tempdir()
            .unwrap();
        let r = Runtime::new().unwrap();
        let mut wb = WriteBatch::new();
        const TOTAL_CASES: usize = 100;
        let mut cf_opt = ColumnFamilyOptions::default();
        cf_opt.write_buffer_size = 128 * 1024;
        util::enable_processing();
        let cfs = vec![
            ColumnFamilyDescriptor {
                name: "default".to_string(),
                options: ColumnFamilyOptions::default(),
            },
            ColumnFamilyDescriptor {
                name: "lock".to_string(),
                options: ColumnFamilyOptions::default(),
            },
            ColumnFamilyDescriptor {
                name: "write".to_string(),
                options: ColumnFamilyOptions::default(),
            },
        ];
        let mut engine = build_db(&dir, cfs);
        let first_switch_wal_number = Arc::new(AtomicU64::new(0));
        let switch_wal_number = first_switch_wal_number.clone();
        util::set_callback("switch_wal", move |v| {
            let x = v.unwrap();
            if switch_wal_number.load(Ordering::Acquire) == 0 {
                switch_wal_number.store(x.parse::<u64>().unwrap(), Ordering::Release);
            }
            None
        });
        let switch_wal_number = first_switch_wal_number.clone();
        let flush_cf = Arc::new(Mutex::new(HashMap::<u32, u64>::new()));
        let cfs = flush_cf.clone();
        util::set_callback("switch_memtable", move |v| {
            let x = v.unwrap().parse::<u64>().unwrap();
            let cf_id = (x % 1000) as u32;
            let log_number = x / 1000;
            if switch_wal_number.load(Ordering::Acquire) != 0 {
                let mut guard = cfs.lock().unwrap();
                if guard.get(&cf_id).is_none() {
                    assert_eq!(log_number, switch_wal_number.load(Ordering::Acquire));
                    guard.insert(cf_id, log_number);
                }
            }
            None
        });
        let cfs = flush_cf.clone();
        let switch_wal_number = first_switch_wal_number.clone();
        util::set_callback("switch_empty_memtable", move |v| {
            let x = v.unwrap().parse::<u64>().unwrap();
            let cf_id = (x % 1000) as u32;
            let log_number = x / 1000;
            if switch_wal_number.load(Ordering::Acquire) == log_number {
                let mut guard = cfs.lock().unwrap();
                guard.insert(cf_id, 0);
            }
            None
        });
        let flush_memtable_count_wal = Arc::new(AtomicU64::new(0));
        let flush_memtable_count = flush_memtable_count_wal.clone();
        let total_flush_memtable_count = Arc::new(AtomicU64::new(0));
        let count1 = total_flush_memtable_count.clone();
        let switch_wal_number = first_switch_wal_number.clone();
        let cfs = flush_cf.clone();
        let (cb, rc) = std::sync::mpsc::channel();
        let block_point = Arc::new(Mutex::new(Option::Some(rc)));
        util::set_callback("run_flush_memtable_job", move |v| {
            let v = v.unwrap().parse::<u64>().unwrap();
            if switch_wal_number.load(Ordering::Acquire) != 0 {
                let guard = cfs.lock().unwrap();
                let cf_id = (v % 1000) as u32;
                if let Some(flush_log_umber) = guard.get(&cf_id) {
                    if *flush_log_umber == v / 1000 {
                        flush_memtable_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            if count1.fetch_add(1, Ordering::SeqCst) == 0 {
                let rc = block_point.lock().unwrap().take().unwrap();
                rc.recv().unwrap();
            }
            None
        });
        for i in 0..TOTAL_CASES {
            for j in 0..100 {
                let k = (i * 100 + j).to_string();
                wb.put_cf(0, k.as_bytes(), b"v00000000000001");
                if j % 3 == 0 {
                    wb.put_cf(1, k.as_bytes(), b"v00000000000001");
                }
            }
            r.block_on(engine.write_opt(&mut wb, false, false)).unwrap();
            wb.clear();
        }
        {
            let vs = engine.version_set.lock().unwrap();
            let v = vs.get_superversion(0).unwrap();
            assert_eq!(2, v.imms.len());
        }
        let _ = cb.send(());
        while flush_memtable_count_wal.load(Ordering::Relaxed) < 2 {
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(flush_memtable_count_wal.load(Ordering::Relaxed), 2);
        let x = { *flush_cf.lock().unwrap().get(&2).unwrap() };
        assert_eq!(x, 0);
        {
            let vs = engine.version_set.lock().unwrap();
            let v = vs.get_superversion(0).unwrap();
            assert_eq!(0, v.imms.len());
        }
    }
}
