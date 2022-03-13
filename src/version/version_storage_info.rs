use crate::common::{extract_user_key, Result};
use crate::iterator::{AsyncIterator, BTreeTableAccessor, TwoLevelIterator};
use crate::options::ReadOptions;
use crate::util::{BTree, BtreeComparable, PageIterator};
use crate::version::TableFile;
use std::sync::Arc;

const MAX_BTREE_PAGE_SIZE: usize = 128;

#[derive(Clone)]
pub struct LevelInfo {
    pub tables: BTree<Arc<TableFile>>,
    pub total_file_size: u64,
    pub level_max_bytes: u64,
}

#[derive(Clone, Default)]
pub struct Level0Info {
    pub tables: Vec<Arc<TableFile>>,
    pub total_file_size: u64,
}

#[derive(Clone, Default)]
pub struct VersionStorageInfo {
    level0: Level0Info,
    levels: Vec<LevelInfo>,
    base_level: usize,
    max_level: usize,
    level_multiplier: f64,
}

impl VersionStorageInfo {
    pub fn new(to_add: Vec<Arc<TableFile>>, max_level: usize) -> Self {
        let info = VersionStorageInfo {
            level0: Level0Info::default(),
            levels: vec![
                LevelInfo {
                    tables: BTree::new(MAX_BTREE_PAGE_SIZE),
                    total_file_size: 0,
                    level_max_bytes: 0
                };
                max_level - 1
            ],
            base_level: 0,
            max_level,
            level_multiplier: 0.0,
        };
        if to_add.is_empty() {
            info
        } else {
            info.apply(to_add, vec![])
        }
    }

    pub fn size(&self) -> usize {
        self.levels.len()
    }

    pub fn get_table_level0_info(&self) -> &Level0Info {
        &self.level0
    }

    pub fn get_base_level_info(&self) -> &[LevelInfo] {
        &self.levels
    }

    pub fn get_base_level(&self) -> usize {
        self.base_level
    }

    pub fn get_level_multiplier(&self) -> f64 {
        self.level_multiplier
    }

    pub fn scan<F: FnMut(&Arc<TableFile>)>(&self, mut consumer: F, level: usize) {
        if level == 0 {
            for f in &self.level0.tables {
                consumer(f);
            }
        } else {
            if level > self.levels.len() {
                return;
            }
            let mut iter = self.levels[level - 1].tables.new_iterator();
            iter.seek_to_first();
            while iter.valid() {
                let r = iter.record().unwrap();
                consumer(&r);
                iter.next();
            }
        }
    }

    pub fn apply(&self, to_add: Vec<Arc<TableFile>>, to_delete: Vec<Arc<TableFile>>) -> Self {
        let mut to_delete_level = vec![vec![]; self.max_level - 1];
        let mut to_add_level = vec![vec![]; self.max_level - 1];
        let mut level0_tables = vec![];
        let mut level0_delete = vec![];
        let mut to_add_file_size = vec![0; self.max_level];
        let mut to_delete_file_size = vec![0; self.max_level];
        for f in to_delete {
            f.mark_removed();
            to_delete_file_size[f.meta.level as usize] += f.meta.fd.file_size;
            if f.meta.level == 0 {
                level0_delete.push(f);
            } else {
                let idx = f.meta.level as usize - 1;
                assert!(idx < self.max_level);
                to_delete_level[idx].push(f);
            }
        }
        for f in &self.level0.tables {
            let mut need_keep = true;
            for x in &level0_delete {
                if x.id() == f.id() {
                    need_keep = false;
                    break;
                }
            }
            if need_keep {
                level0_tables.push(f.clone());
            }
        }
        for f in to_add {
            to_add_file_size[f.meta.level as usize] += f.meta.fd.file_size;
            if f.meta.level == 0 {
                level0_tables.push(f);
            } else {
                let idx = f.meta.level as usize - 1;
                assert!(idx < self.max_level);
                to_add_level[idx].push(f);
            }
        }

        let l = std::cmp::max(to_delete_level.len(), to_add_level.len());
        assert_eq!(l, self.levels.len());
        let mut levels = vec![];
        for i in 0..l {
            let add = if i < to_add_level.len() && !to_add_level[i].is_empty() {
                std::mem::take(&mut to_add_level[i])
            } else {
                vec![]
            };
            let del = if i + 1 < to_delete_level.len() && !to_delete_level[i].is_empty() {
                std::mem::take(&mut to_delete_level[i])
            } else {
                vec![]
            };
            if add.is_empty() && del.is_empty() {
                levels.push(self.levels[i].clone());
            } else {
                let tree = self.levels[i].tables.replace(del, add);
                levels.push(LevelInfo {
                    tables: tree,
                    total_file_size: self.levels[i].total_file_size + to_add_file_size[i + 1]
                        - to_delete_file_size[i + 1],
                    level_max_bytes: self.levels[i].level_max_bytes,
                });
            }
        }

        Self {
            level0: Level0Info {
                tables: level0_tables,
                total_file_size: self.level0.total_file_size + to_add_file_size[0]
                    - to_delete_file_size[0],
            },
            levels,
            max_level: self.max_level,
            base_level: self.base_level,
            level_multiplier: 1.0,
        }
    }

    fn get_table(&self, key: &[u8], level: usize) -> Option<Arc<TableFile>> {
        self.levels[level].tables.get(key)
    }

    pub fn append_iterator_to(&self, opts: &ReadOptions, iters: &mut Vec<Box<dyn AsyncIterator>>) {
        let l = self.level0.tables.len();
        for i in 0..l {
            iters.push(self.level0.tables[l - i - 1].reader.new_iterator_opts(opts));
        }
        for info in self.levels.iter() {
            if info.tables.size() > 0 {
                let iter = info.tables.new_iterator();
                let accessor = BTreeTableAccessor::new(iter);
                let iter = TwoLevelIterator::new(accessor);
                iters.push(Box::new(iter));
            }
        }
    }

    pub fn get_level0_file_num(&self) -> usize {
        self.level0.tables.len()
    }

    pub fn update_base_bytes(
        &mut self,
        max_bytes_for_level_base: u64,
        level0_file_num_compaction_trigger: usize,
        max_bytes_for_level_multiplier: f64,
    ) {
        let mut max_level_size = 0;
        let mut first_non_empty_level = None;
        for i in 0..self.levels.len() {
            let total_size = self.levels[i].total_file_size;
            if total_size > 0 && first_non_empty_level.is_none() {
                first_non_empty_level = Some(i + 1);
            }
            max_level_size = std::cmp::max(max_level_size, total_size);
            self.levels[i].level_max_bytes = u64::MAX;
        }
        if max_level_size == 0 {
            self.base_level = self.max_level - 1;
        } else {
            assert!(first_non_empty_level.is_some());
            let first_non_empty_level = first_non_empty_level.unwrap();
            let l0_size = self.level0.total_file_size;
            let base_bytes_max = std::cmp::max(l0_size, max_bytes_for_level_base);
            let base_bytes_min =
                (base_bytes_max as f64 / max_bytes_for_level_multiplier).round() as u64;
            let mut cur_level_size = max_level_size;
            for _ in first_non_empty_level..(self.max_level - 1) {
                cur_level_size =
                    (cur_level_size as f64 / max_bytes_for_level_multiplier).round() as u64;
            }
            let mut base_level_size = if cur_level_size <= base_bytes_min {
                self.base_level = first_non_empty_level;
                base_bytes_min + 1
            } else {
                self.base_level = first_non_empty_level;
                while self.base_level > 1 && cur_level_size > base_bytes_max {
                    self.base_level -= 1;
                    cur_level_size =
                        (cur_level_size as f64 / max_bytes_for_level_multiplier).round() as u64;
                }
                std::cmp::min(base_bytes_max, cur_level_size)
            };
            self.level_multiplier = max_bytes_for_level_multiplier;
            if l0_size > base_level_size
                && (l0_size > max_bytes_for_level_base
                    || self.level0.tables.len() / 2 > level0_file_num_compaction_trigger)
            {
                base_level_size = l0_size;
            }
            if self.base_level == self.max_level - 1 {
                self.level_multiplier = 1.0;
            } else {
                self.level_multiplier = max_level_size as f64 / base_level_size as f64;
                self.level_multiplier = self
                    .level_multiplier
                    .powf(1.0 / (self.max_level - self.base_level - 1) as f64);
            }
            let mut level_size = base_level_size;
            for i in self.base_level..self.max_level {
                if i > self.base_level && ((u64::MAX / level_size) as f64) > self.level_multiplier {
                    level_size = (level_size as f64 * self.level_multiplier).round() as u64;
                }
                self.levels[i - 1].level_max_bytes = std::cmp::max(level_size, base_bytes_max);
            }
        }
    }

    pub async fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let l = self.level0.tables.len();
        for i in 0..l {
            if let Some(v) = self.level0.tables[l - i - 1].reader.get(opts, key).await? {
                return Ok(Some(v));
            }
        }
        let l = self.levels.len();
        for i in 0..l {
            if self.levels[i].tables.size() == 0 {
                continue;
            }
            if let Some(table) = self.levels[i].tables.get(extract_user_key(key)) {
                if let Some(v) = table.reader.get(opts, key).await? {
                    return Ok(Some(v));
                }
            }
        }
        Ok(None)
    }

    pub fn get_overlap_with_compaction(
        &self,
        level: u32,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<Arc<TableFile>> {
        let mut tables = vec![];
        // TODO: compare with user_comparator
        if level == 0 {
            for t in &self.level0.tables {
                if t.largest().ge(smallest) && t.smallest().le(largest) {
                    tables.push(t.clone());
                }
            }
        } else {
            let idx = level as usize - 1;
            let mut iter = self.levels[idx].tables.new_iterator();
            iter.seek(smallest);
            while iter.valid() {
                let t = iter.record().unwrap();
                if t.smallest().gt(largest) {
                    break;
                }
                tables.push(t);
                iter.next();
            }
        }
        tables
    }
}
