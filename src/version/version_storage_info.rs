use crate::common::Result;
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
}

impl VersionStorageInfo {
    pub fn new(to_add: Vec<Arc<TableFile>>, max_level: usize) -> Self {
        let info = VersionStorageInfo {
            level0: Level0Info::default(),
            levels: vec![
                LevelInfo {
                    tables: BTree::new(MAX_BTREE_PAGE_SIZE),
                    total_file_size: 0
                };
                max_level
            ],
            base_level: 0,
            max_level,
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

    pub fn scan<F: FnMut(&Arc<TableFile>)>(&self, mut consumer: F, level: usize) {
        if level == 0 {
            for f in &self.level0.tables {
                consumer(f);
            }
        } else {
            if level >= self.levels.len() + 1 {
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
        let mut to_delete_level = vec![vec![]; self.max_level];
        let mut to_add_level = vec![vec![]; self.max_level];
        let mut level0_tables = vec![];
        let mut level0_delete = vec![];
        let mut to_add_file_size = vec![0; self.max_level];
        let mut to_delete_file_size = vec![0; self.max_level];
        for f in to_delete {
            f.mark_removed();
            if f.meta.level == 0 {
                level0_delete.push(f);
            } else {
                to_delete_file_size[f.meta.level as usize] += f.meta.fd.file_size;
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
                    // The file size of delete tables may not set. So use self info instead.
                    to_delete_file_size[0] += f.meta.fd.file_size;
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
        assert!(l <= self.levels.len());
        let mut levels = vec![];
        let mut base_level = 0;
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
                if self.levels[i].tables.size() > 0 {
                    base_level = i + 1;
                }
                levels.push(self.levels[i].clone());
            } else {
                let tree = self.levels[i].tables.replace(del, add);
                if tree.size() > 0 && base_level == 0 {
                    base_level = i + 1;
                }
                levels.push(LevelInfo {
                    tables: tree,
                    total_file_size: self.levels[i].total_file_size + to_add_file_size[i + 1]
                        - to_delete_file_size[i + 1],
                });
            }
        }
        if base_level == 0 {
            base_level = self.base_level;
        }

        Self {
            level0: Level0Info {
                tables: level0_tables,
                total_file_size: self.level0.total_file_size + to_add_file_size[0]
                    - to_delete_file_size[0],
            },
            levels,
            max_level: self.max_level,
            base_level,
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

    pub async fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let l = self.level0.tables.len();
        for i in 0..l {
            if let Some(v) = self.level0.tables[l - i - 1].reader.get(opts, key).await? {
                return Ok(Some(v));
            }
        }
        let l = self.levels.len();
        for i in 0..l {
            if let Some(table) = self.levels[i].tables.get(key) {
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
                if t.smallest().lt(largest) {
                    break;
                }
                tables.push(t);
                iter.next();
            }
        }
        tables
    }
}
