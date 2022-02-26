mod compaction_iter;
mod compaction_job;
mod flush_job;

use crate::common::Result;
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyOptions, ImmutableDBOptions};
use crate::version::{TableFile, Version, VersionEdit};
use std::sync::Arc;

pub use flush_job::run_flush_memtable_job;

#[async_trait::async_trait]
pub trait CompactionEngine: Clone + Sync + Send {
    async fn apply(&mut self, edits: Vec<VersionEdit>) -> Result<()>;
}

pub struct CompactionRequest {
    input: Vec<(u32, Vec<Arc<TableFile>>)>,
    input_version: Arc<Version>,
    cf: u32,
    output_level: u32,
    cf_options: Arc<ColumnFamilyOptions>,
    options: Arc<ImmutableDBOptions>,
    target_file_size_base: usize,
}

pub struct FlushRequest {
    mems: Vec<(u32, Arc<Memtable>)>,
}

impl FlushRequest {
    pub fn new(mems: Vec<(u32, Arc<Memtable>)>) -> Self {
        Self { mems }
    }
}
