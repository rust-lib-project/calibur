use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};
use futures::SinkExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::common::{make_log_file, Error, IOOption, Result};
use crate::compaction::{CompactionEngine, FlushRequest};
use crate::log::LogWriter;
use crate::manifest::ManifestScheduler;
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::version::{KernelNumberContext, VersionEdit, VersionSet};
use crate::write_batch::ReadOnlyWriteBatch;
use crate::{ColumnFamilyOptions, FileSystem, KeyComparator};

pub struct IngestFile {
    pub file: PathBuf,
    pub column_family: u32,
}

pub struct WriteMemtableTask {
    pub wb: ReadOnlyWriteBatch,
    pub mems: Vec<(u32, Arc<Memtable>)>,
}

impl WriteMemtableTask {
    pub fn check_memtable_cf(&self, cf: u32) -> usize {
        let mut idx = cf as usize;
        if idx >= self.mems.len() || cf != self.mems[idx].0 {
            idx = self.mems.len();
            for i in 0..self.mems.len() {
                if self.mems[i].0 == cf {
                    idx = i;
                    break;
                }
            }
            if idx == self.mems.len() {
                panic!("write miss column family");
            }
        }
        idx
    }
}

pub enum WALTask {
    Write {
        wb: ReadOnlyWriteBatch,
        cb: OnceSender<Result<WriteMemtableTask>>,
        sync: bool,
        disable_wal: bool,
    },
    Ingest {
        files: Vec<IngestFile>,
        cb: OnceSender<Result<()>>,
    },
    CreateColumnFamily {
        name: String,
        opts: ColumnFamilyOptions,
        cb: OnceSender<Result<u32>>,
    },
}

pub struct WALContext {
    last_sequence: u64,
    buf: Vec<u8>,
}

pub struct WALWriter {
    kernel: Arc<KernelNumberContext>,
    writer: Box<LogWriter>,
    logs: Vec<Box<LogWriter>>,
    mems: Vec<(u32, Arc<Memtable>)>,
    version_sets: Arc<Mutex<VersionSet>>,
    flush_scheduler: UnboundedSender<FlushRequest>,
    manifest_scheduler: ManifestScheduler,
    immutation_options: Arc<ImmutableDBOptions>,
    ctx: WALContext,
}

impl WALWriter {
    pub fn new(
        kernel: Arc<KernelNumberContext>,
        version_sets: Arc<Mutex<VersionSet>>,
        immutation_options: Arc<ImmutableDBOptions>,
        flush_scheduler: UnboundedSender<FlushRequest>,
        manifest_scheduler: ManifestScheduler,
    ) -> Result<Self> {
        let writer = Self::create_wal(
            kernel.as_ref(),
            &immutation_options.db_path,
            immutation_options.fs.as_ref(),
        )?;
        let last_sequence = kernel.last_sequence();
        let mems = {
            let vs = version_sets.lock().unwrap();
            vs.get_column_family_memtables()
        };
        let wal = WALWriter {
            kernel,
            writer,
            flush_scheduler,
            manifest_scheduler,
            logs: vec![],
            mems,
            version_sets,
            ctx: WALContext {
                last_sequence,
                buf: vec![],
            },
            immutation_options,
        };
        Ok(wal)
    }

    fn create_wal(
        kernel: &KernelNumberContext,
        path: &str,
        fs: &dyn FileSystem,
    ) -> Result<Box<LogWriter>> {
        let log_number = kernel.new_file_number();
        let fname = make_log_file(path, log_number);
        let mut opts = IOOption::default();
        opts.high_priority = true;
        opts.buffer_size = 1024 * 64;
        let writer = fs.open_writable_file_writer_opt(fname, &opts)?;
        Ok(Box::new(LogWriter::new(writer, log_number)))
    }

    pub async fn preprocess_write(&mut self) -> Result<()> {
        let mut new_log_writer = false;
        let mut switch_wal = false;
        if self.writer.get_file_size() > self.immutation_options.max_total_wal_size {
            new_log_writer = true;
            switch_wal = true;
        }
        // TODO: check atomic flush.
        for (cf, mem) in &mut self.mems {
            if (switch_wal && !mem.is_empty()) || mem.should_flush() {
                // If this method returns false, it means that another write thread still hold this
                // memtable. Maybe we shall also check the previous memtables has been flushed.
                new_log_writer = true;
                if self.writer.get_file_size() > 0 {
                    self.writer.fsync().await?;
                    let new_writer = Self::create_wal(
                        &self.kernel,
                        &self.immutation_options.db_path,
                        self.immutation_options.fs.as_ref(),
                    )?;
                    let writer = std::mem::replace(&mut self.writer, new_writer);
                    self.logs.push(writer);
                }
                mem.set_next_log_number(self.writer.get_log_number());
                let mut vs = self.version_sets.lock().unwrap();
                *mem = vs.switch_memtable(*cf, self.kernel.last_sequence());
            }
        }

        let mut mems = vec![];
        if new_log_writer {
            let mut vs = self.version_sets.lock().unwrap();
            vs.schedule_immutable_memtables(&mut mems);
        }
        if !mems.is_empty() {
            let _ = self
                .flush_scheduler
                .send(FlushRequest::new(mems, self.ctx.last_sequence))
                .await;
        }
        Ok(())
    }

    pub async fn create_column_family(
        &mut self,
        name: String,
        opts: ColumnFamilyOptions,
    ) -> Result<u32> {
        {
            let mut vs = self.version_sets.lock().unwrap();
            if vs.mut_column_family_by_name(&name).is_some() {
                return Err(Error::Config(format!(
                    "Column family [{}] already exists",
                    name
                )));
            }
        }
        let new_id = self.kernel.next_column_family_id();
        let mut edit = VersionEdit::default();
        edit.set_max_column_family(new_id);
        edit.set_log_number(self.writer.get_log_number());
        edit.add_column_family(name);
        edit.column_family = new_id;
        edit.set_comparator_name(opts.comparator.name());
        edit.cf_options.options = Some(opts);
        self.manifest_scheduler.apply(vec![edit]).await?;
        let vs = self.version_sets.lock().unwrap();
        self.mems
            .push((new_id, vs.get_superversion(new_id).unwrap().mem.clone()));
        Ok(new_id)
    }

    pub async fn write(
        &mut self,
        tasks: &mut Vec<(
            ReadOnlyWriteBatch,
            OnceSender<Result<WriteMemtableTask>>,
            bool,
        )>,
        sync_wal: bool,
    ) -> Result<Vec<(u32, Arc<Memtable>)>> {
        if tasks.is_empty() {
            return Ok(vec![]);
        }
        let l = tasks.len();
        for (wb, _, disable_wal) in tasks {
            let sequence = self.ctx.last_sequence + 1;
            self.ctx.last_sequence = self.ctx.last_sequence + wb.count() as u64;
            wb.set_sequence(sequence);
            if !*disable_wal {
                if l == 1 {
                    self.writer.add_record(wb.get_data()).await?;
                } else {
                    self.ctx.buf.extend_from_slice(wb.get_data());
                }
            }
        }
        if !self.ctx.buf.is_empty() {
            self.writer.add_record(&self.ctx.buf).await?;
            self.ctx.buf.clear();
        }

        if sync_wal {
            self.writer.fsync().await?;
        }
        Ok(self.mems.clone())
    }
}

#[derive(Clone)]
pub struct WALScheduler {
    sender: UnboundedSender<WALTask>,
}

impl WALScheduler {
    pub fn new(sender: UnboundedSender<WALTask>) -> Self {
        Self { sender }
    }

    pub async fn schedule_create_column_family(
        &mut self,
        name: &str,
        opts: ColumnFamilyOptions,
    ) -> Result<u32> {
        let (cb, rx) = once_channel();
        let task = WALTask::CreateColumnFamily {
            name: name.to_string(),
            opts,
            cb,
        };
        self.sender
            .send(task)
            .await
            .map_err(|_| Error::Cancel("wal"))?;
        let ret = rx.await.map_err(|_| Error::Cancel("wal"))?;
        ret
    }

    pub async fn schedule_writebatch(
        &mut self,
        wb: ReadOnlyWriteBatch,
        sync: bool,
        disable_wal: bool,
    ) -> Result<WriteMemtableTask> {
        let (cb, rx) = once_channel();
        let task = WALTask::Write {
            wb,
            cb,
            sync,
            disable_wal,
        };
        self.sender
            .send(task)
            .await
            .map_err(|_| Error::Cancel("wal"))?;
        let wb = rx.await.map_err(|_| Error::Cancel("wal"))?;
        wb
    }
}
