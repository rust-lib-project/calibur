use crate::compaction::CompactionRequest;
use crate::util::{BtreeComparable, PageIterator};
use crate::version::{TableFile, Version};
use crate::{ColumnFamilyOptions, ImmutableDBOptions};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

pub struct LevelCompactionPicker {
    cf_opts: HashMap<u32, Arc<ColumnFamilyOptions>>,
    db_opts: Arc<ImmutableDBOptions>,
}

impl LevelCompactionPicker {
    pub fn new(
        cf_opts: HashMap<u32, Arc<ColumnFamilyOptions>>,
        db_opts: Arc<ImmutableDBOptions>,
    ) -> Self {
        Self { cf_opts, db_opts }
    }

    pub fn pick_compaction(&self, cf: u32, version: Arc<Version>) -> Option<CompactionRequest> {
        let opts = self.cf_opts.get(&cf).cloned().unwrap();
        let (mut scores, base_level) =
            self.calculate_compaction_score(version.as_ref(), opts.as_ref());
        if scores.is_empty() {
            return None;
        } else {
            scores.sort_by(|a, b| {
                if a.1 > b.1 {
                    return Ordering::Less;
                } else {
                    return Ordering::Greater;
                }
            });
            let (level, score) = scores.first().unwrap();
            if *score < 1.1 {
                return None;
            }
            let output_level = if *level == 0 { base_level } else { *level + 1 };
            let mut input = self.pick_input_files_from_level(
                version.as_ref(),
                opts.as_ref(),
                *level,
                output_level,
            );
            if input.is_empty() {
                return None;
            }
            for (_, f) in input.iter_mut() {
                f.mark_compaction();
            }
            let request = CompactionRequest {
                input,
                input_version: version,
                cf,
                output_level,
                cf_options: opts,
                options: self.db_opts.clone(),
                target_file_size_base: 0,
            };
            Some(request)
        }
    }

    fn calculate_compaction_score(
        &self,
        version: &Version,
        opts: &ColumnFamilyOptions,
    ) -> (Vec<(u32, f64)>, u32) {
        let info = version.get_storage_info();
        let mut score_and_level = vec![];
        let level0_info = info.get_table_level0_info();
        let mut being_compact = false;
        for t in &level0_info.tables {
            if t.is_pending_compaction() {
                being_compact = true;
                break;
            }
        }
        // TODO: support intra level 0 compaction
        if !being_compact {
            score_and_level.push((
                0,
                level0_info.tables.len() as f64 / opts.level0_file_num_compaction_trigger as f64,
            ));
        }
        let base_levels = info.get_base_level_info();
        let mut base_level = opts.max_level - 1;
        if base_levels.last().unwrap().tables.size() > 0 {
            let mut db_size = base_levels.last().unwrap().total_file_size as f64;
            let l = base_levels.len();
            for i in 0..l {
                let idx = l - i - 1;
                if base_levels[idx].tables.size() == 0 {
                    break;
                }
                if base_levels[idx].total_file_size <= opts.max_bytes_for_level_base as u64 {
                    base_level = idx as u32 + 1;
                } else if idx > 0
                    && base_levels[idx].total_file_size as f64
                        > opts.max_bytes_for_level_base as f64 * 1.2
                {
                    base_level = idx as u32;
                }
                score_and_level.push((
                    idx as u32 + 1,
                    base_levels[idx].total_file_size as f64 / db_size,
                ));
                db_size /= opts.max_bytes_for_level_multiplier;
            }
        }
        (score_and_level, base_level)
    }

    fn pick_input_files_from_level(
        &self,
        version: &Version,
        opts: &ColumnFamilyOptions,
        level: u32,
        output_level: u32,
    ) -> Vec<(u32, Arc<TableFile>)> {
        let info = version.get_storage_info();
        let mut start_inputs = vec![];
        let user_comparator = opts.comparator.get_user_comparator();
        let mut largest = vec![];
        let mut smallest = vec![];
        if level == 0 {
            let level0 = info.get_table_level0_info();
            for t in &level0.tables {
                if largest.is_empty() || user_comparator.less_than(&largest, t.largest()) {
                    largest = t.largest().to_vec();
                }
                if smallest.is_empty() || user_comparator.less_than(t.smallest(), &smallest) {
                    smallest = t.smallest().to_vec();
                }
                start_inputs.push((0, t.clone()));
            }
        } else if level + 1 < opts.max_level {
            let idx = level as usize - 1;
            let mut iter = {
                let levels = info.get_base_level_info();
                levels[idx].tables.new_iterator()
            };
            iter.seek_to_first();
            let mut table_scores = vec![];
            while iter.valid() {
                if let Some(table) = iter.record() {
                    if !table.is_pending_compaction() {
                        let overlapping_files = info.get_overlap_with_compaction(
                            level + 1,
                            table.smallest(),
                            table.largest(),
                        );
                        let mut being_compact = false;
                        let mut overlapping_size = table.meta.fd.file_size;
                        for t in &overlapping_files {
                            if t.is_pending_compaction() {
                                being_compact = true;
                                break;
                            }
                            overlapping_size += t.meta.fd.file_size;
                        }
                        if being_compact {
                            continue;
                        }
                        table_scores.push((table.clone(), overlapping_size));
                    }
                }
                iter.next();
            }
            table_scores.sort_by(|a, b| a.1.cmp(&b.1));
            if table_scores.is_empty() {
                return vec![];
            }
            let table = table_scores.first().unwrap().0.clone();
            smallest = table.smallest().to_vec();
            largest = table.largest().to_vec();
            start_inputs.push((level, table));
        } else {
            // TODO: support bottommost level compaction
        }

        let mut expand_inputs = vec![];
        let mut next_level_smallest = smallest.clone();
        let mut next_level_largest = largest.clone();
        if !self.expand_input_files_from_level(
            version,
            &mut next_level_smallest,
            &mut next_level_largest,
            output_level,
            &mut expand_inputs,
        ) {
            return vec![];
        }
        start_inputs.extend_from_slice(&expand_inputs);
        start_inputs
    }

    fn expand_input_files_from_level(
        &self,
        version: &Version,
        smallest: &mut Vec<u8>,
        largest: &mut Vec<u8>,
        level: u32,
        input_files: &mut Vec<(u32, Arc<TableFile>)>,
    ) -> bool {
        assert!(level > 0);
        let overlapping_files = version
            .get_storage_info()
            .get_overlap_with_compaction(level, smallest, largest);
        let mut being_compact = false;
        for t in &overlapping_files {
            if t.is_pending_compaction() {
                being_compact = true;
                break;
            }
            input_files.push((level, t.clone()));
        }
        if being_compact {
            input_files.clear();
            return false;
        }
        true
    }
}
