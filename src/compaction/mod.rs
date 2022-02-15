mod compaction_iter;
mod flush_job;

use crate::common::{InternalKeyComparator, Result};
use crate::compaction::flush_job::FlushJob;
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::table::InternalIterator;
use crate::version::{KernelNumberContext, VersionEdit};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait CompactionEngine: Clone + Sync + Send {
    async fn apply(&mut self, edits: Vec<VersionEdit>) -> Result<()>;
    fn new_merging_iterator(&self, mems: &[Arc<Memtable>]) -> Box<dyn InternalIterator>;
}

pub enum CompactionRequest {
    Flush(Vec<Arc<Memtable>>),
    Compaction,
}

pub struct FlushRequest {
    mems: Vec<(u32, Arc<Memtable>)>,
}

async fn run_flush_memtable_job<Engine: CompactionEngine>(
    mut engine: Engine,
    reqs: Vec<FlushRequest>,
    versions: Arc<KernelNumberContext>,
    options: Arc<ImmutableDBOptions>,
    comparator: InternalKeyComparator,
) -> Result<()> {
    let mut mems = vec![];
    for req in &reqs {
        for (cf, mem) in &req.mems {
            while *cf >= mems.len() as u32 {
                mems.push(vec![]);
            }
            mems[(*cf) as usize].push(mem.clone());
        }
    }
    let mut edits = vec![];
    for i in 0..mems.len() {
        if !mems[i].is_empty() {
            let file_number = versions.new_file_number();
            let memids = mems[i].iter().map(|mem| mem.get_id()).collect();
            let mut job = FlushJob::new(
                engine.clone(),
                options.clone(),
                mems[i].clone(),
                comparator.clone(),
                i as u32,
                file_number,
            );
            let meta = job.run().await?;
            let mut edit = VersionEdit::default();
            edit.prev_log_number = 0;
            edit.log_number = mems[i].last().unwrap().get_next_log_number();
            edit.add_file(
                0,
                file_number,
                meta.fd.file_size,
                meta.smallest.as_ref(),
                meta.largest.as_ref(),
                meta.fd.smallest_seqno,
                meta.fd.largest_seqno,
            );
            edit.mems_deleted = memids;
            edit.column_family = i as u32;
            edits.push(edit);
        }
    }
    engine.apply(edits).await
}
