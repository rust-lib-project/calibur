use crate::common::options::CompressionType;
use crate::common::{make_table_file_name, InternalKeyComparator, Result};
use crate::compaction::compaction_iter::CompactionIter;
use crate::compaction::CompactionEngine;
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyOptions, ImmutableDBOptions};
use crate::table::{InternalIterator, MergingIterator, TableBuilderOptions};
use crate::version::{FileMetaData, VersionEdit};
use std::sync::Arc;

pub struct FlushJob<E: CompactionEngine> {
    engine: E,
    options: Arc<ImmutableDBOptions>,
    cf_options: Arc<ColumnFamilyOptions>,
    version_edit: VersionEdit,
    mems: Vec<Arc<Memtable>>,
    meta: FileMetaData,
    comparator: InternalKeyComparator,
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
        }
    }

    fn new_merging_iterator(&self, mems: &[Arc<Memtable>]) -> Box<dyn InternalIterator> {
        if mems.len() == 1 {
            return mems[0].new_iterator();
        } else {
            let iters = mems.iter().map(|mem| mem.new_iterator()).collect();
            let iter = MergingIterator::new(iters, mems[0].get_comparator());
            Box::new(iter)
        }
    }

    pub async fn run(&mut self) -> Result<FileMetaData> {
        let fname = make_table_file_name(&self.options.db_path, self.meta.id());
        let file = self.options.fs.open_writable_file(fname)?;
        let mut build_opts = TableBuilderOptions::default();
        build_opts.skip_filter = false;
        build_opts.column_family_id = self.cf_id;
        build_opts.compression_type = CompressionType::NoCompression;
        build_opts.target_file_size = 0;
        build_opts.internal_comparator = self.comparator.clone();
        let mut builder = self.cf_options.factory.new_builder(&build_opts, file)?;
        let mut iter = self.new_merging_iterator(&self.mems);
        iter.seek_to_first();
        let mut compact_iter = CompactionIter::new(
            iter,
            self.comparator.get_user_comparator().clone(),
            vec![],
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
        self.meta.fd.file_size = builder.file_size();
        Ok(self.meta.clone())
    }
}
