use crate::version::column_family::ColumnFamily;
use std::sync::{atomic::AtomicU64, Arc};

pub struct VersionSet {
    next_file_number: AtomicU64,
    column_family_set: Vec<Option<Arc<ColumnFamily>>>,
}

impl VersionSet {
    pub fn get_column_family(&self, id: usize) -> Option<Arc<ColumnFamily>> {
        if id > self.column_family_set.len() {
            None
        } else {
            self.column_family_set[id].clone()
        }
    }

    pub fn set_column_family(&mut self, cf: Arc<ColumnFamily>) {
        if cf.get_id() >= self.column_family_set.len() {
            self.column_family_set.resize(cf.get_id() + 1, None);
        }
        self.column_family_set[cf.get_id()] = Some(cf);
    }

    pub fn should_flush(&self) -> bool {
        for cf in self.column_family_set.iter() {
            if cf.map_or(false, |cf| cf.should_flush()) {
                return true;
            }
        }
        false
    }

    pub fn get_column_familys(&self) -> Vec<Arc<ColumnFamily>> {
        let mut cfs = vec![];
        for c in self.column_family_set.iter() {
            if let Some(cf) = c {
                cfs.push(cf.clone());
            }
        }
        cfs
    }
}
