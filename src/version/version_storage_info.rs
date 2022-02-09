use crate::util::{BTree, BtreeComparable, PageIterator};
use crate::version::{FileMetaData, VersionEdit};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MAX_BTREE_PAGE_SIZE: usize = 128;

#[derive(Clone, Default)]
pub struct VersionStorageInfo {
    level0: Vec<FileMetaData>,
    base_level: Vec<BTree<FileMetaData>>,

    // only used for delete
    files_by_id: Arc<Mutex<HashMap<u64, FileMetaData>>>,
}

impl VersionStorageInfo {
    pub fn new(edits: Vec<VersionEdit>) -> Self {
        let info = VersionStorageInfo {
            level0: vec![],
            base_level: vec![],
            files_by_id: Arc::new(Mutex::new(HashMap::default())),
        };
        info.apply(edits)
    }

    pub fn size(&self) -> usize {
        self.base_level.len()
    }

    pub fn scan<F: FnMut(&FileMetaData)>(&self, mut consumer: F, level: usize) {
        if level == 0 {
            for f in &self.level0 {
                consumer(f);
            }
        } else {
            if level >= self.base_level.len() + 1 {
                return;
            }
            let mut iter = self.base_level[level - 1].new_iterator();
            iter.seek_to_first();
            while iter.valid() {
                let r = iter.record().unwrap();
                consumer(&r);
            }
        }
    }

    pub fn apply(&self, mut edits: Vec<VersionEdit>) -> Self {
        let mut to_deletes = vec![vec![]; self.base_level.len() + 1];
        let mut to_add = vec![vec![]; self.base_level.len() + 1];
        let mut files_index = self.files_by_id.lock().unwrap();
        for e in &mut edits {
            for f in e.add_files.drain(..) {
                while to_add.len() <= f.level as usize {
                    to_add.push(vec![]);
                }
                files_index.insert(f.id(), f.clone());
                to_add[f.level as usize].push(f);
            }
            for f in e.deleted_files.drain(..) {
                while to_deletes.len() <= f.level as usize {
                    to_deletes.push(vec![]);
                }
                if let Some(old_f) = files_index.remove(&f.id()) {
                    to_deletes[f.level as usize].push(old_f);
                }
            }
        }
        let mut level0 = vec![];
        let mut base_level = vec![];

        for f in &self.level0 {
            let mut need_keep = true;
            for x in &to_deletes[0] {
                if x.id() == f.id() {
                    need_keep = false;
                    break;
                }
            }
            if need_keep {
                level0.push(f.clone());
            }
        }
        for f in to_add[0].drain(..) {
            level0.push(f);
        }
        level0.sort_by(|a, b| a.fd.smallest_seqno.cmp(&b.fd.smallest_seqno));

        for i in 0..self.base_level.len() {
            if to_add[i + 1].is_empty() && to_deletes[i + 1].is_empty() {
                base_level.push(self.base_level[i].clone());
                continue;
            }
            base_level.push(self.base_level[i].replace(
                std::mem::take(&mut to_deletes[i + 1]),
                std::mem::take(&mut to_add[i + 1]),
            ));
        }
        let l = std::cmp::max(to_add.len(), to_deletes.len()) - 1;
        for i in self.base_level.len()..l {
            let mut tree = BTree::new(MAX_BTREE_PAGE_SIZE);
            let add = if i + 1 < to_add.len() && !to_add[i + 1].is_empty() {
                std::mem::take(&mut to_add[i + 1])
            } else {
                vec![]
            };
            let del = if i + 1 < to_deletes.len() && !to_deletes[i + 1].is_empty() {
                std::mem::take(&mut to_deletes[i + 1])
            } else {
                vec![]
            };
            tree = tree.replace(del, add);
            base_level.push(tree);
        }

        Self {
            level0,
            base_level,
            files_by_id: self.files_by_id.clone(),
        }
    }
}
