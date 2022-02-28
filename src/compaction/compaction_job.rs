use std::sync::Arc;

use crate::common::format::extract_user_key;
use crate::common::CompressionType;
use crate::common::{make_table_file_name, Result};
use crate::compaction::compaction_iter::CompactionIter;
use crate::compaction::{CompactionEngine, CompactionRequest};
use crate::iterator::{AsyncIterator, AsyncMergingIterator, TwoLevelIterator, VecTableAccessor};
use crate::table::TableBuilderOptions;
use crate::util::BtreeComparable;
use crate::version::{FileMetaData, KernelNumberContext, VersionEdit};
use crate::{ColumnFamilyOptions, ImmutableDBOptions};

pub async fn run_compaction_job<Engine: CompactionEngine>(
    mut engine: Engine,
    request: CompactionRequest,
    kernel: Arc<KernelNumberContext>,
    options: Arc<ImmutableDBOptions>,
    cf_options: Arc<ColumnFamilyOptions>,
) -> Result<()> {
    let mut iters: Vec<Box<dyn AsyncIterator>> = vec![];
    for (_level, tables) in request.input.iter() {
        let accessor = VecTableAccessor::new(tables.clone());
        let two_level_iter = TwoLevelIterator::new(accessor);
        iters.push(Box::new(two_level_iter));
    }
    let iter = AsyncMergingIterator::new(iters, cf_options.comparator.clone());
    let user_comparator = cf_options.comparator.get_user_comparator().clone();
    let mut compact_iter =
        CompactionIter::new_with_async(Box::new(iter), user_comparator.clone(), vec![], false);
    compact_iter.seek_to_first().await;

    let mut meta = FileMetaData::new(
        kernel.new_file_number(),
        request.output_level,
        vec![],
        vec![],
    );
    let fname = make_table_file_name(&options.db_path, meta.id());
    let file = options.fs.open_writable_file_writer(fname)?;
    let mut build_opts = TableBuilderOptions::default();
    build_opts.skip_filter = false;
    build_opts.column_family_id = request.cf;
    build_opts.compression_type = CompressionType::NoCompression;
    build_opts.target_file_size = 0;
    build_opts.internal_comparator = cf_options.comparator.clone();

    let mut builder = cf_options.factory.new_builder(&build_opts, file)?;
    let mut metas = vec![];
    while compact_iter.valid() {
        let key = compact_iter.key();
        let value = compact_iter.value();
        if builder.file_size() > request.target_file_size_base as u64
            && !user_comparator
                .same_key(extract_user_key(builder.last_key()), extract_user_key(key))
        {
            builder.finish().await?;
            meta.fd.file_size = builder.file_size();
            metas.push(meta);
            meta = FileMetaData::new(
                kernel.new_file_number(),
                request.output_level,
                vec![],
                vec![],
            );
            let fname = make_table_file_name(&options.db_path, meta.id());
            let file = options.fs.open_writable_file_writer(fname)?;
            builder = cf_options.factory.new_builder(&build_opts, file)?;
        } else if builder.should_flush() {
            builder.flush().await?;
        }
        builder.add(key, value)?;
        meta.update_boundary(key, compact_iter.current_sequence());
        compact_iter.next().await;
    }
    drop(compact_iter);
    builder.finish().await?;
    meta.fd.file_size = builder.file_size();
    metas.push(meta);
    let mut edit = VersionEdit::default();
    // edit.prev_log_number = 0;
    // edit.set_log_number(mems[i].last().unwrap().get_next_log_number());
    for m in metas {
        edit.add_file(
            0,
            m.id(),
            m.fd.file_size,
            m.smallest.as_ref(),
            m.largest.as_ref(),
            m.fd.smallest_seqno,
            m.fd.largest_seqno,
        );
    }
    for (level, tables) in request.input.iter() {
        for t in tables.iter() {
            edit.delete_file(*level, t.id());
        }
    }
    edit.column_family = request.cf;
    engine.apply(vec![edit]).await
}
