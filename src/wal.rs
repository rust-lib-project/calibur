use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::Sender as OnceSender;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::common::{make_log_file, Result};
use crate::log::LogWriter;
use crate::options::ImmutableDBOptions;
use crate::version::{KernelNumberContext, VersionSet};
use crate::write_batch::ReadOnlyWriteBatch;
use crate::FileSystem;

const MAX_BATCH_SIZE: usize = 1 << 20; // 1MB

pub struct IngestFile {
    pub file: PathBuf,
    pub column_family: u32,
}

pub enum WALTask {
    Write {
        wb: ReadOnlyWriteBatch,
        cb: OnceSender<Result<ReadOnlyWriteBatch>>,
        sync: bool,
        disable_wal: bool,
    },
    Ingest {
        files: Vec<IngestFile>,
        cb: OnceSender<Result<()>>,
    },
}

pub struct WALWriter {
    kernel: Arc<KernelNumberContext>,
    writer: Box<LogWriter>,
    logs: Vec<Box<LogWriter>>,
    version_sets: Arc<Mutex<VersionSet>>,
    last_sequence: u64,
    immutation_options: Arc<ImmutableDBOptions>,
    buf: Vec<u8>,
}

impl WALWriter {
    pub fn new(
        kernel: Arc<KernelNumberContext>,
        version_sets: Arc<Mutex<VersionSet>>,
        immutation_options: Arc<ImmutableDBOptions>,
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
            logs: vec![],
            version_sets,
            last_sequence,
            immutation_options,
            buf: vec![],
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

    pub fn batch(&mut self, wb: &mut ReadOnlyWriteBatch, disable_wal: bool) -> bool {
        let sequence = self.last_sequence + 1;
        wb.set_sequence(sequence);
        if !disable_wal {
            wb.append_to(&mut self.buf);
        }
        self.last_sequence = sequence;
        self.buf.len() > MAX_BATCH_SIZE || disable_wal
    }

    pub async fn write(&mut self, wb: &mut ReadOnlyWriteBatch, disable_wal: bool) -> Result<()> {
        let sequence = self.last_sequence + 1;
        wb.set_sequence(sequence);
        self.last_sequence = sequence;
        if !disable_wal {
            self.writer.add_record(wb.get_data()).await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.writer.add_record(&self.buf).await?;
            self.buf.clear();
        }
        Ok(())
    }

    pub async fn fsync(&mut self) -> Result<()> {
        self.writer.fsync().await
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
}
