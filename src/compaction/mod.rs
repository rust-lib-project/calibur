mod compaction_iter;
mod compaction_job;
mod flush_job;
mod picker;

use crate::common::Result;
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyOptions, ImmutableDBOptions};
use crate::version::{TableFile, Version, VersionEdit};
use std::sync::Arc;

pub use compaction_job::run_compaction_job;
pub use flush_job::run_flush_memtable_job;
pub use picker::LevelCompactionPicker;

#[async_trait::async_trait]
pub trait CompactionEngine: Clone + Sync + Send {
    async fn apply(&mut self, edits: Vec<VersionEdit>) -> Result<()>;
}

pub struct CompactionRequest {
    input: Vec<(u32, Arc<TableFile>)>,
    input_version: Arc<Version>,
    cf: u32,
    output_level: u32,
    cf_options: Arc<ColumnFamilyOptions>,
    options: Arc<ImmutableDBOptions>,
    target_file_size_base: usize,
}

pub struct FlushRequest {
    pub mems: Vec<(u32, Arc<Memtable>)>,
    pub wait_commit_request: u64,
}

impl FlushRequest {
    pub fn new(mems: Vec<(u32, Arc<Memtable>)>, wait_commit_request: u64) -> Self {
        Self {
            mems,
            wait_commit_request,
        }
    }
}
