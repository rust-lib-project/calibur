use std::collections::HashMap;
use std::sync::Arc;

use crate::common::format::extract_user_key;
use crate::common::CompressionType;
use crate::common::{make_table_file_name, Result};
use crate::compaction::compaction_iter::CompactionIter;
use crate::compaction::{CompactionEngine, CompactionRequest};
use crate::iterator::{AsyncIterator, AsyncMergingIterator, TwoLevelIterator, VecTableAccessor};
use crate::table::{TableBuilderOptions, TableCache};
use crate::util::BtreeComparable;
use crate::version::{FileMetaData, KernelNumberContext, TableFile, VersionEdit};

pub async fn run_compaction_job<Engine: CompactionEngine>(
    mut engine: Engine,
    request: CompactionRequest,
    kernel: Arc<KernelNumberContext>,
    table_cache: Arc<TableCache>,
) -> Result<()> {
    let mut iters: Vec<Box<dyn AsyncIterator>> = vec![];
    let mut level_tables: HashMap<u32, Vec<Arc<TableFile>>> = HashMap::default();
    for (level, f) in request.input.iter() {
        if *level > 0 {
            if let Some(files) = level_tables.get_mut(level) {
                files.push(f.clone());
            } else {
                level_tables.insert(*level, vec![f.clone()]);
            }
        } else {
            let reader = table_cache.get_table_reader(&f.meta).await?;
            // do not handle the reader because iterator will handle a table-rep.
            iters.push(reader.value().new_iterator());
        }
    }
    for (_level, mut tables) in level_tables {
        if tables.len() > 1 {
            let accessor = VecTableAccessor::new(tables);
            let two_level_iter = TwoLevelIterator::new(accessor, table_cache.clone());
            iters.push(Box::new(two_level_iter));
        } else {
            let table = tables.pop().unwrap();
            let reader = table_cache.get_table_reader(&table.meta).await?;
            iters.push(reader.value().new_iterator());
        }
    }
    let iter = AsyncMergingIterator::new(iters, request.cf_options.comparator.clone());
    let user_comparator = request.cf_options.comparator.get_user_comparator().clone();
    let mut compact_iter =
        CompactionIter::new_with_async(Box::new(iter), user_comparator.clone(), vec![], false);
    compact_iter.seek_to_first().await;

    let mut meta = FileMetaData::new(
        kernel.new_file_number(),
        request.output_level,
        vec![],
        vec![],
    );
    let fname = make_table_file_name(&request.options.db_path, meta.id());
    let file = request.options.fs.open_writable_file_writer(&fname)?;
    let mut build_opts = TableBuilderOptions::default();
    build_opts.skip_filter = false;
    build_opts.column_family_id = request.cf;
    build_opts.compression_type = CompressionType::NoCompression;
    build_opts.target_file_size = 0;
    build_opts.internal_comparator = request.cf_options.comparator.clone();

    let mut builder = request.cf_options.factory.new_builder(&build_opts, file)?;
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
            let fname = make_table_file_name(&request.options.db_path, meta.id());
            let file = request.options.fs.open_writable_file_writer(&fname)?;
            builder = request.cf_options.factory.new_builder(&build_opts, file)?;
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
    meta.num_entries = builder.num_entries();
    metas.push(meta);
    let mut edit = VersionEdit::default();
    // edit.prev_log_number = 0;
    // edit.set_log_number(mems[i].last().unwrap().get_next_log_number());
    for m in metas {
        edit.add_file(
            request.output_level,
            m.id(),
            m.fd.file_size,
            m.smallest.as_ref(),
            m.largest.as_ref(),
            m.fd.smallest_seqno,
            m.fd.largest_seqno,
        );
    }
    for (level, table) in request.input.iter() {
        edit.delete_file(*level, table.id());
    }
    edit.column_family = request.cf;
    engine.apply(vec![edit]).await
}
