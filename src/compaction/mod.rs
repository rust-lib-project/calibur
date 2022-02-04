mod compaction_iter;
mod flush_job;

use crate::common::{InternalKeyComparator, Result};
use crate::compaction::flush_job::FlushJob;
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::table::InternalIterator;
use crate::version::{VersionEdit, VersionSetKernal};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait CompactionEngine: Clone + Sync + Send {
    async fn apply(&self, version: Vec<VersionEdit>);
    fn get_options(&self) -> Arc<ImmutableDBOptions>;
    fn new_merging_iterator(&self, mems: &[Arc<Memtable>]) -> Box<dyn InternalIterator>;
    fn get_comparator(&self, cf: u32) -> InternalKeyComparator;
}

pub enum CompactionRequest {
    Flush(Vec<Arc<Memtable>>),
    Compaction,
}

pub struct FlushRequest {
    mems: Vec<(u32, Arc<Memtable>)>,
}

async fn run_flush_memtable_job<Engine: CompactionEngine>(
    engine: Engine,
    reqs: Vec<FlushRequest>,
    versions: Arc<VersionSetKernal>,
) -> Result<()> {
    let mut mems = vec![];
    let options = engine.get_options();
    for req in &reqs {
        for (cf, mem) in &req.mems {
            while *cf >= mems.len() as u32 {
                mems.push(vec![]);
            }
            mems[(*cf) as usize].push(mem.clone());
        }
    }
    for i in 0..mems.len() {
        if !mems[i].is_empty() {
            let file_number = versions.new_file_number();
            let mut job = FlushJob::new(
                engine.clone(),
                options.clone(),
                mems[i].clone(),
                i as u32,
                file_number,
            );
            job.run().await?;
        }
    }
    Ok(())
}
