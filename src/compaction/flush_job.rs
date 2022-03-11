use crate::common::CompressionType;
use crate::common::{make_table_file_name, InternalKeyComparator, Result};
use crate::compaction::compaction_iter::CompactionIter;
use crate::compaction::{CompactionEngine, FlushRequest};
use crate::iterator::{InternalIterator, MergingIterator};
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyOptions, ImmutableDBOptions};
use crate::table::TableBuilderOptions;
use crate::version::{FileMetaData, KernelNumberContext, VersionEdit};
use std::collections::HashMap;
use std::sync::Arc;

pub struct FlushJob<E: CompactionEngine> {
    engine: E,
    options: Arc<ImmutableDBOptions>,
    cf_options: Arc<ColumnFamilyOptions>,
    version_edit: VersionEdit,
    mems: Vec<Arc<Memtable>>,
    meta: FileMetaData,
    comparator: InternalKeyComparator,
    snapshots: Vec<u64>,
    cf_id: u32,
}

impl<E: CompactionEngine> FlushJob<E> {
    pub fn new(
        engine: E,
        options: Arc<ImmutableDBOptions>,
        cf_options: Arc<ColumnFamilyOptions>,
        mems: Vec<Arc<Memtable>>,
        comparator: InternalKeyComparator,
        cf_id: u32,
        file_number: u64,
        snapshots: Vec<u64>,
    ) -> Self {
        let mut version_edit = VersionEdit::default();
        version_edit.column_family = cf_id;
        version_edit.mems_deleted = mems.iter().map(|m| m.get_id()).collect();
        version_edit.prev_log_number = 0;
        version_edit.set_log_number(mems.last().unwrap().get_next_log_number());
        let meta = FileMetaData::new(file_number, 0, vec![], vec![]);
        Self {
            engine,
            options,
            version_edit,
            mems,
            cf_id,
            meta,
            comparator,
            cf_options,
            snapshots,
        }
    }

    fn new_merging_iterator(&self, mems: &[Arc<Memtable>]) -> Box<dyn InternalIterator> {
        if mems.len() == 1 {
            mems[0].new_iterator()
        } else {
            let iters = mems.iter().map(|mem| mem.new_iterator()).collect();
            let iter = MergingIterator::new(iters, mems[0].get_comparator());
            Box::new(iter)
        }
    }

    pub async fn run(&mut self) -> Result<FileMetaData> {
        let fname = make_table_file_name(&self.options.db_path, self.meta.id());
        let file = self.options.fs.open_writable_file_writer(&fname)?;
        let mut build_opts = TableBuilderOptions::default();
        build_opts.skip_filter = false;
        build_opts.column_family_id = self.cf_id;
        build_opts.compression_type = CompressionType::NoCompression;
        build_opts.target_file_size = 0;
        build_opts.internal_comparator = self.comparator.clone();
        let mut builder = self.cf_options.factory.new_builder(&build_opts, file)?;
        let iter = self.new_merging_iterator(&self.mems);
        let mut compact_iter = CompactionIter::new(
            iter,
            self.comparator.get_user_comparator().clone(),
            self.snapshots.clone(),
            false,
        );
        compact_iter.seek_to_first().await;
        while compact_iter.valid() {
            let key = compact_iter.key();
            let value = compact_iter.value();
            if builder.should_flush() {
                builder.flush().await?;
            }
            builder.add(key, value)?;
            self.meta
                .update_boundary(key, compact_iter.current_sequence());
            compact_iter.next().await;
        }
        drop(compact_iter);
        builder.finish().await?;
        self.meta.num_entries = builder.num_entries();
        self.meta.fd.file_size = builder.file_size();
        Ok(self.meta.clone())
    }
}

pub async fn run_flush_memtable_job<Engine: CompactionEngine>(
    mut engine: Engine,
    reqs: Vec<FlushRequest>,
    kernel: Arc<KernelNumberContext>,
    options: Arc<ImmutableDBOptions>,
    cf_options: HashMap<u32, Arc<ColumnFamilyOptions>>,
    snapshots: Vec<u64>,
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
    for (i, memtables) in mems.iter().enumerate() {
        if !memtables.is_empty() {
            let file_number = kernel.new_file_number();
            let memids = memtables.iter().map(|mem| mem.get_id()).collect();
            let idx = i as u32;
            let cf_opt = cf_options
                .get(&idx)
                .cloned()
                .unwrap_or_else(|| Arc::new(ColumnFamilyOptions::default()));
            let comparator = cf_opt.comparator.clone();
            let mut job = FlushJob::new(
                engine.clone(),
                options.clone(),
                cf_opt,
                memtables.clone(),
                comparator,
                i as u32,
                file_number,
                snapshots.clone(),
            );
            let meta = job.run().await?;
            let mut edit = VersionEdit::default();
            edit.prev_log_number = 0;
            edit.set_log_number(memtables.last().unwrap().get_next_log_number());
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
