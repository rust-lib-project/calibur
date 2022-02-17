use crate::util::{BTree, BtreeComparable, PageIterator};
use crate::version::{FileMetaData, TableFile};
use std::sync::Arc;

const MAX_BTREE_PAGE_SIZE: usize = 128;

#[derive(Clone, Default)]
pub struct VersionStorageInfo {
    level0: Vec<Arc<TableFile>>,
    base_level: Vec<BTree<Arc<TableFile>>>,
}

impl VersionStorageInfo {
    pub fn new(to_add: Vec<Arc<TableFile>>) -> Self {
        let info = VersionStorageInfo {
            level0: vec![],
            base_level: vec![],
        };
        if to_add.is_empty() {
            info
        } else {
            info.apply(to_add, vec![])
        }
    }

    pub fn size(&self) -> usize {
        self.base_level.len()
    }

    pub fn scan<F: FnMut(&FileMetaData)>(&self, mut consumer: F, level: usize) {
        if level == 0 {
            for f in &self.level0 {
                consumer(&f.meta);
            }
        } else {
            if level >= self.base_level.len() + 1 {
                return;
            }
            let mut iter = self.base_level[level - 1].new_iterator();
            iter.seek_to_first();
            while iter.valid() {
                let r = iter.record().unwrap();
                consumer(&r.meta);
            }
        }
    }

    pub fn apply(&self, to_add: Vec<Arc<TableFile>>, to_delete: Vec<Arc<TableFile>>) -> Self {
        let mut to_delete_level = vec![vec![]; self.base_level.len()];
        let mut to_add_level = vec![vec![]; self.base_level.len()];
        let mut level0 = vec![];
        let mut level0_delete = vec![];
        for f in to_delete {
            if f.meta.level == 0 {
                level0_delete.push(f);
            } else {
                let idx = f.meta.level as usize - 1;
                while to_delete_level.len() <= idx {
                    to_delete_level.push(vec![]);
                }
                to_delete_level[idx].push(f);
            }
        }
        for f in &self.level0 {
            let mut need_keep = true;
            for x in &level0_delete {
                if x.id() == f.id() {
                    need_keep = false;
                    break;
                }
            }
            if need_keep {
                level0.push(f.clone());
            }
        }
        for f in to_add {
            if f.meta.level == 0 {
                level0.push(f);
            } else {
                let idx = f.meta.level as usize - 1;
                while to_add_level.len() <= idx {
                    to_add_level.push(vec![]);
                }
                to_add_level[idx].push(f);
            }
        }

        let l = std::cmp::max(to_delete_level.len(), to_add_level.len());
        let l = std::cmp::max(l, self.base_level.len());
        let mut base_level = vec![];
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
            if add.is_empty() && del.is_empty() && i < self.base_level.len() {
                base_level.push(self.base_level[i].clone());
            } else if i < self.base_level.len() {
                base_level.push(self.base_level[i].replace(del, add));
            } else {
                base_level.push(BTree::new(MAX_BTREE_PAGE_SIZE));
            }
        }

        Self { level0, base_level }
    }
}
