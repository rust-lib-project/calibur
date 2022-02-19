use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};
use futures::SinkExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::common::{make_log_file, Error, Result};
use crate::compaction::FlushRequest;
use crate::log::LogWriter;
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::version::{KernelNumberContext, VersionSet};
use crate::write_batch::ReadOnlyWriteBatch;
use crate::FileSystem;

const MAX_BATCH_SIZE: usize = 1 << 20; // 1MB

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
}

pub struct WALContext {
    need_sync: bool,
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
    immutation_options: Arc<ImmutableDBOptions>,
    ctx: WALContext,
}

impl WALWriter {
    pub fn new(
        kernel: Arc<KernelNumberContext>,
        version_sets: Arc<Mutex<VersionSet>>,
        immutation_options: Arc<ImmutableDBOptions>,
        flush_scheduler: UnboundedSender<FlushRequest>,
    ) -> Result<Self> {
        let writer = Self::create_wal(
            kernel.as_ref(),
            &immutation_options.db_path,
            immutation_options.fs.as_ref(),
        )?;
        let last_sequence = kernel.last_sequence();
        let wal = WALWriter {
            kernel,
            writer,
            flush_scheduler,
            logs: vec![],
            mems: vec![],
            version_sets,
            ctx: WALContext {
                need_sync: false,
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
        let writer = fs.open_writable_file(fname)?;
        Ok(Box::new(LogWriter::new(writer, log_number)))
    }

    pub async fn preprocess_write(&mut self) -> Result<()> {
        let mut new_log_writer = false;
        if self.writer.get_file_size() > self.immutation_options.max_total_wal_size {
            new_log_writer = true;
        }
        // TODO: check atomic flush.
        for (cf, mem) in &mut self.mems {
            if mem.should_flush() {
                // If this method returns false, it means that another write thread still hold this
                // memtable. Maybe we shall also check the previous memtables has been flushed.
                if mem.mark_schedule_flush() {
                    let _ = self
                        .flush_scheduler
                        .send(FlushRequest::new(*cf, mem.clone()))
                        .await;
                }
                {
                    let mut vs = self.version_sets.lock().unwrap();
                    *mem = vs.switch_memtable(*cf);
                }
                new_log_writer = true;
            }
        }
        if new_log_writer {
            self.writer.fsync().await?;
            let new_writer = Self::create_wal(
                &self.kernel,
                &self.immutation_options.db_path,
                self.immutation_options.fs.as_ref(),
            )?;
            let writer = std::mem::replace(&mut self.writer, new_writer);
            self.logs.push(writer);
        }
        Ok(())
    }

    pub fn batch(
        &mut self,
        mut wb: ReadOnlyWriteBatch,
        disable_wal: bool,
        sync: bool,
    ) -> WriteMemtableTask {
        let sequence = self.ctx.last_sequence + 1;
        self.ctx.last_sequence = sequence;
        wb.set_sequence(sequence);
        if !disable_wal {
            wb.append_to(&mut self.ctx.buf);
        }
        if sync {
            self.ctx.need_sync = true;
        }

        for (_cf, m) in &self.mems {
            m.mark_write_begin();
        }

        WriteMemtableTask {
            wb,
            mems: self.mems.clone(),
        }
    }

    pub fn should_flush(&self) -> bool {
        self.ctx.buf.len() > MAX_BATCH_SIZE || self.ctx.buf.is_empty()
    }

    pub async fn flush(&mut self) -> Result<()> {
        if !self.ctx.buf.is_empty() {
            self.writer.add_record(&self.ctx.buf).await?;
            self.ctx.buf.clear();
        }
        Ok(())
    }

    pub async fn fsync(&mut self) -> Result<()> {
        if !self.ctx.need_sync {
            self.writer.fsync().await?;
            self.ctx.need_sync = false;
        }
        Ok(())
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

    pub async fn send(
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
            .map_err(|_| Error::Cancel(format!("wal")))?;
        let wb = rx.await.map_err(|_| Error::Cancel(format!("wal")))?;
        wb
    }
}
