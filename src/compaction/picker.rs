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
        let mut ctx = LevelCompactionPriorityContext::default();
        ctx.calculate_compaction_score(version.as_ref(), opts.as_ref());
        if let Some((start_level, output_level)) = ctx.pickup_high_priority_level() {
            let mut input = ctx.pick_input_files_from_level(
                version.as_ref(),
                opts.as_ref(),
                start_level,
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
                target_file_size_base: opts.target_file_size_base,
                cf_options: opts,
                options: self.db_opts.clone(),
            };
            Some(request)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct LevelCompactionPriorityContext {
    level_and_scores: Vec<(u32, f64)>,
    base_level: u32,
    max_level: u32,
}

impl LevelCompactionPriorityContext {
    fn calculate_compaction_score(&mut self, version: &Version, opts: &ColumnFamilyOptions) {
        let info = version.get_storage_info();
        let mut level0_being_compact = false;
        for t in &info.get_table_level0_info().tables {
            if t.is_pending_compaction() {
                level0_being_compact = true;
                break;
            }
        }
        let mut l0_target_size = opts.max_bytes_for_level_base;
        let base_level = info.get_base_level();
        // TODO: support intra level 0 compaction
        if !level0_being_compact && info.get_level_multiplier() > 0.01 {
            assert!(base_level > 0);
            let level_max_bytes = info.get_base_level_info()[base_level - 1].level_max_bytes as f64
                / info.get_level_multiplier();
            l0_target_size = std::cmp::max(l0_target_size, level_max_bytes.round() as u64);
            let score = info.get_table_level0_info().tables.len() as f64
                / opts.level0_file_num_compaction_trigger as f64;
            self.level_and_scores.push((
                0,
                f64::max(
                    score,
                    info.get_table_level0_info().total_file_size as f64 / l0_target_size as f64,
                ),
            ));
        }
        let l = info.get_base_level_info().len();
        for i in 0..l - 1 {
            let mut level_bytes_no_compacting = 0;
            info.scan(
                |f| {
                    if !f.is_pending_compaction() {
                        // TODO: use compensated_file_size instead.
                        level_bytes_no_compacting += f.meta.fd.file_size;
                    }
                },
                i + 1,
            );
            self.level_and_scores.push((
                i as u32 + 1,
                level_bytes_no_compacting as f64
                    / info.get_base_level_info()[i].level_max_bytes as f64,
            ));
        }
        self.base_level = info.get_base_level() as u32;
        self.max_level = info.get_base_level_info().len() as u32 + 1;
    }

    fn pickup_high_priority_level(&mut self) -> Option<(u32, u32)> {
        self.level_and_scores.sort_by(|a, b| {
            if a.1 > b.1 {
                return Ordering::Less;
            } else {
                return Ordering::Greater;
            }
        });
        let base_level = self.base_level;
        if let Some((level, score)) = self.level_and_scores.first() {
            if *score < 1.0 {
                return None;
            } else if *level == 0 {
                return Some((*level, base_level));
            } else if *level + 1 == self.max_level {
                return Some((*level, *level));
            } else {
                return Some((*level, *level + 1));
            }
        }
        None
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
                        let mut overlapping_size = 0;
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
                        // TODO: use sum(next_level_file.file_size) / table.meta.fd.compensated_file_size to order.
                        table_scores
                            .push((table.clone(), overlapping_size / table.meta.fd.file_size));
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
            unimplemented!();
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::InMemFileSystem;
    use crate::table::InMemTableReader;
    use crate::version::FileMetaData;
    use crate::{DBOptions, KeyComparator};
    use std::path::PathBuf;

    fn generate_version(
        opts: &ColumnFamilyOptions,
        fs: &Arc<InMemFileSystem>,
        table_info: Vec<(u64, u64, u32, Vec<u8>, Vec<u8>)>,
    ) -> Version {
        let tables = table_info
            .into_iter()
            .map(|(id, file_size, level, smallest, largest)| {
                let reader = InMemTableReader::new(vec![]);
                let mut m = FileMetaData::new(id, level, smallest, largest);
                m.fd.file_size = file_size;
                Arc::new(TableFile::new(
                    m,
                    Box::new(reader),
                    fs.clone(),
                    PathBuf::from(id.to_string()),
                ))
            })
            .collect();
        Version::new(
            0,
            "default".to_string(),
            opts.comparator.name().to_string(),
            tables,
            0,
            7,
        )
    }

    fn gen_key(k: &[u8]) -> Vec<u8> {
        let mut v = k.to_vec();
        v.extend_from_slice(&0u64.to_le_bytes());
        v
    }

    fn update_version(version: &mut Version, opts: &Arc<ColumnFamilyOptions>) {
        version.update_base_bytes(
            opts.max_bytes_for_level_base,
            opts.level0_file_num_compaction_trigger,
            opts.max_bytes_for_level_multiplier,
        );
    }

    #[test]
    fn test_compaction_picker_l0() {
        let fs = Arc::new(InMemFileSystem::default());
        let mut opts = ColumnFamilyOptions::default();
        opts.max_bytes_for_level_base = 400;
        opts.level0_file_num_compaction_trigger = 4;
        let opts = Arc::new(opts);

        // trigger level0 to base level compaction when base level is empty
        let db_opts = DBOptions::default();
        let immutable_opts = Arc::new(ImmutableDBOptions::from(db_opts));
        let mut v = generate_version(
            opts.as_ref(),
            &fs,
            vec![
                (0, 100, 0, gen_key(b"aa"), gen_key(b"ca")),
                (1, 100, 0, gen_key(b"ba"), gen_key(b"da")),
                (2, 100, 0, gen_key(b"ca"), gen_key(b"da")),
                (3, 100, 0, gen_key(b"aa"), gen_key(b"ba")),
            ],
        );
        update_version(&mut v, &opts);
        let mut cf_opts = HashMap::default();
        cf_opts.insert(0, opts.clone());
        let picker = LevelCompactionPicker::new(cf_opts, immutable_opts);
        let request = picker.pick_compaction(0, Arc::new(v)).unwrap();
        assert_eq!(request.input.len(), 4);
        assert_eq!(request.output_level, opts.max_level - 1);

        let mut v = generate_version(
            opts.as_ref(),
            &fs,
            vec![
                (4, 100, 6, gen_key(b"aa"), gen_key(b"aa")),
                (5, 100, 6, gen_key(b"ba"), gen_key(b"ba")),
                (6, 100, 6, gen_key(b"ca"), gen_key(b"ca")),
                (7, 100, 6, gen_key(b"da"), gen_key(b"da")),
                (8, 100, 0, gen_key(b"aa"), gen_key(b"ca")),
                (9, 100, 0, gen_key(b"ba"), gen_key(b"da")),
                (10, 100, 0, gen_key(b"ca"), gen_key(b"da")),
                (11, 100, 0, gen_key(b"aa"), gen_key(b"ba")),
            ],
        );
        update_version(&mut v, &opts);
        let request = picker.pick_compaction(0, Arc::new(v)).unwrap();
        assert_eq!(request.input.len(), 8);
        assert_eq!(request.output_level, opts.max_level - 1);

        // The files in base level are too large so that place the new file in lower level.
        let mut v = generate_version(
            opts.as_ref(),
            &fs,
            vec![
                (1, 200, 6, gen_key(b"aa"), gen_key(b"aa")),
                (2, 200, 6, gen_key(b"ba"), gen_key(b"ba")),
                (3, 100, 6, gen_key(b"ca"), gen_key(b"ca")),
                (4, 100, 6, gen_key(b"da"), gen_key(b"da")),
                (8, 100, 0, gen_key(b"aa"), gen_key(b"ca")),
                (9, 100, 0, gen_key(b"ba"), gen_key(b"da")),
                (10, 100, 0, gen_key(b"ca"), gen_key(b"da")),
                (11, 100, 0, gen_key(b"aa"), gen_key(b"ba")),
            ],
        );
        update_version(&mut v, &opts);
        assert_eq!(v.get_storage_info().get_base_level(), 5);
        let request = picker.pick_compaction(0, Arc::new(v)).unwrap();
        assert_eq!(request.input.len(), 4);
        assert_eq!(request.output_level, opts.max_level - 2);
    }

    #[test]
    fn test_compaction_picker_l1() {
        let fs = Arc::new(InMemFileSystem::default());
        let mut opts = ColumnFamilyOptions::default();
        opts.max_bytes_for_level_base = 200;
        opts.level0_file_num_compaction_trigger = 2;
        let opts = Arc::new(opts);
        let immutable_opts = Arc::new(ImmutableDBOptions::from(DBOptions::default()));
        let mut cf_opts = HashMap::default();
        cf_opts.insert(0, opts.clone());
        let picker = LevelCompactionPicker::new(cf_opts, immutable_opts);

        let mut v = generate_version(
            opts.as_ref(),
            &fs,
            vec![
                (1, 1, 0, gen_key(b"150"), gen_key(b"200")),
                (2, 1, 0, gen_key(b"200"), gen_key(b"250")),
                (3, 300, 6, gen_key(b"200"), gen_key(b"250")),
                (4, 300, 6, gen_key(b"300"), gen_key(b"350")),
                (5, 3, 4, gen_key(b"150"), gen_key(b"180")),
                (6, 3, 4, gen_key(b"181"), gen_key(b"350")),
                (7, 3, 4, gen_key(b"400"), gen_key(b"450")),
            ],
        );
        update_version(&mut v, &opts);
        assert_eq!(v.get_storage_info().get_base_level(), 4);
        let request = picker.pick_compaction(0, Arc::new(v)).unwrap();
        assert_eq!(request.output_level, opts.max_level - 3);
        assert_eq!(request.input.len(), 4);
        assert_eq!(request.input[0].1.id(), 1);
        assert_eq!(request.input[1].1.id(), 2);
        assert_eq!(request.input[2].1.id(), 5);
        assert_eq!(request.input[3].1.id(), 6);

        // The files in base level are too large so that place the new file in lower level.
        let mut v = generate_version(
            opts.as_ref(),
            &fs,
            vec![
                (1, 1, 0, gen_key(b"150"), gen_key(b"200")),
                (2, 300, 6, gen_key(b"200"), gen_key(b"250")),
                (3, 3000, 6, gen_key(b"300"), gen_key(b"350")),
                (4, 3, 6, gen_key(b"400"), gen_key(b"450")),
                (5, 300, 5, gen_key(b"150"), gen_key(b"180")),
                (6, 500, 5, gen_key(b"181"), gen_key(b"350")),
                (7, 200, 5, gen_key(b"400"), gen_key(b"450")),
            ],
        );
        update_version(&mut v, &opts);
        let request = picker.pick_compaction(0, Arc::new(v)).unwrap();
        assert_eq!(request.input[0].1.id(), 5);
        assert_eq!(request.output_level, opts.max_level - 1);
        assert_eq!(request.input.len(), 1);
    }
}
