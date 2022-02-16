use crate::common::{
    make_log_file, parse_file_name, DBFileType, Error, InternalKeyComparator, Result,
};
use crate::options::{ColumnFamilyDescriptor, DBOptions, ImmutableDBOptions};
use crate::version::{KernelNumberContext, SuperVersion, VersionSet};
use futures::channel::mpsc::unbounded;
use std::path::PathBuf;

use crate::common::format::ValueType;
use crate::log::LogReader;
use crate::manifest::{Manifest, ManifestScheduler, ManifestWriter};
use crate::memtable::Memtable;
use crate::write_batch::{ReadOnlyWriteBatch, WriteBatchItem};
use futures::StreamExt;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use yatp::{task::future::TaskCell, Builder as PoolBuilder, ThreadPool};

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
    kernel: Arc<KernelNumberContext>,
    pool: Arc<ThreadPool<TaskCell>>,
    options: Arc<ImmutableDBOptions>,
    version_cache: Vec<Option<Arc<SuperVersion>>>,
    manifest_scheduler: ManifestScheduler,
}

impl Engine {
    pub async fn open(
        db_options: DBOptions,
        cfs: Vec<ColumnFamilyDescriptor>,
        other_pool: Option<Arc<ThreadPool<TaskCell>>>,
    ) -> Result<Self> {
        Self::recover(db_options, cfs, other_pool).await
    }

    async fn recover(
        db_options: DBOptions,
        cfs: Vec<ColumnFamilyDescriptor>,
        other_pool: Option<Arc<ThreadPool<TaskCell>>>,
    ) -> Result<Self> {
        let immutable_options = Arc::new(db_options.into());
        let manifest = Manifest::recover(&cfs, &immutable_options).await?;
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
        let manifest_scheduler =
            Self::run_manifest_job(&pool, version_set.clone(), Box::new(manifest))?;
        let mut engine = Engine {
            version_set,
            kernel,
            pool,
            options: immutable_options,
            version_cache: vec![],
            manifest_scheduler,
        };
        engine.recover_log(logs).await?;
        Ok(engine)
    }

    fn get_super_version(&mut self, cf: u32) -> Result<Arc<SuperVersion>> {
        let idx = cf as usize;
        while idx >= self.version_cache.len() {
            self.version_cache.push(None);
        }
        if let Some(v) = self.version_cache[idx].as_ref() {
            if v.valid.load(Ordering::Acquire) {
                return Ok(v.clone());
            }
        }
        let vs = self.version_set.lock().unwrap();
        if let Some(v) = vs.get_superversion(idx) {
            self.version_cache[idx] = Some(v.clone());
            Ok(v)
        } else {
            self.version_cache[idx] = None;
            Err(Error::Other(format!("Column family {} not exist", cf)))
        }
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
            while log_reader.read_record(&mut buf).await? {
                let wb = ReadOnlyWriteBatch::try_from(buf.clone())?;
                let sequence = wb.get_sequence();
                self.write_memtable(&wb, sequence)?;
                self.kernel.set_last_sequence(sequence);
            }
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
            let v = self.get_super_version(cf)?;
            mems.push(Some(v.mem.clone()));
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

    fn run_manifest_job(
        pool: &ThreadPool<TaskCell>,
        versions: Arc<Mutex<VersionSet>>,
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
        let engine = ManifestScheduler::new(tx, versions);
        Ok(engine)
    }
}
