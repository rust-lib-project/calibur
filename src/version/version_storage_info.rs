use crate::util::{BTree, BtreeComparable};
use crate::version::{FileMetaData, VersionEdit};

#[derive(Clone)]
pub struct VersionStorageInfo {
    level0: Vec<FileMetaData>,
    base_level: Vec<BTree<FileMetaData>>,
    max_page_size: usize,
}

impl VersionStorageInfo {
    pub fn apply(&self, mut edits: Vec<VersionEdit>) -> Self {
        let mut to_deletes = vec![vec![]; self.base_level.len() + 1];
        let mut to_add = vec![vec![]; self.base_level.len() + 1];
        for e in &mut edits {
            for f in e.deleted_files.drain(..) {
                while to_deletes.len() <= f.level as usize {
                    to_deletes.push(vec![]);
                }
                to_deletes[f.level as usize].push(f);
            }
            for f in e.add_files.drain(..) {
                while to_add.len() <= f.level as usize {
                    to_add.push(vec![]);
                }
                to_add[f.level as usize].push(f);
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
            let mut tree = BTree::new(self.max_page_size);
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
            max_page_size: self.max_page_size,
        }
    }
}
