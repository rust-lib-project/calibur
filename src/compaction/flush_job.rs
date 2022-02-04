use crate::common::options::CompressionType;
use crate::common::{make_table_file_name, Result};
use crate::compaction::CompactionEngine;
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::table::TableBuilderOptions;
use crate::util::BtreeComparable;
use crate::version::{FileMetaData, VersionEdit};
use std::sync::Arc;

pub struct FlushJob<E: CompactionEngine> {
    engine: E,
    options: Arc<ImmutableDBOptions>,
    version_edit: VersionEdit,
    mems: Vec<Arc<Memtable>>,
    meta: FileMetaData,
    cf_id: u32,
}

impl<E: CompactionEngine> FlushJob<E> {
    pub fn new(
        engine: E,
        options: Arc<ImmutableDBOptions>,
        mems: Vec<Arc<Memtable>>,
        cf_id: u32,
        file_number: u64,
    ) -> Self {
        let mut version_edit = VersionEdit::default();
        version_edit.column_family = cf_id;
        version_edit.mems_deleted = mems.iter().map(|m| m.get_id()).collect();
        version_edit.prev_log_number = 0;
        version_edit.log_number = mems.last().unwrap().get_next_log_number();
        let meta = FileMetaData::new(file_number, 0, vec![], vec![]);
        Self {
            engine,
            options,
            version_edit,
            mems,
            cf_id,
            meta,
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
        build_opts.internal_comparator = self.engine.get_comparator(self.cf_id);
        let mut builder = self.options.factory.new_builder(&build_opts, file)?;
        let mut iter = self.engine.new_merging_iterator(&self.mems);
        Ok(self.meta.clone())
    }
}
