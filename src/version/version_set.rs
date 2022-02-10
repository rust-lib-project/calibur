use crate::common::{Error, InternalKeyComparator, KeyComparator, Result};
use crate::log::LogReader;
use crate::memtable::Memtable;
use crate::options::ImmutableDBOptions;
use crate::version::column_family::ColumnFamily;
use crate::version::{Version, VersionEdit};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{atomic, Arc};

#[derive(Default)]
pub struct VersionSetKernel {
    next_file_number: atomic::AtomicU64,
    next_mem_number: atomic::AtomicU64,
    last_sequence: atomic::AtomicU64,
}

impl VersionSetKernel {
    pub fn current_next_file_number(&self) -> u64 {
        self.next_file_number.load(atomic::Ordering::Acquire)
    }

    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, atomic::Ordering::SeqCst)
    }

    pub fn new_memtable_number(&self) -> u64 {
        self.next_mem_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(atomic::Ordering::Acquire)
    }

    pub fn fetch_add_file_number(&self, n: u64) -> u64 {
        self.next_file_number.fetch_add(n, atomic::Ordering::SeqCst)
    }

    pub fn set_last_sequence(&self, v: u64) {
        self.last_sequence.store(v, atomic::Ordering::Release);
    }

    pub fn mark_file_number_used(&self, v: u64) {
        let mut old = self.next_file_number.load(atomic::Ordering::Acquire);
        while old < v {
            match self.next_file_number.compare_exchange(
                old,
                v,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(x) => old = x,
            }
        }
    }
}

pub struct VersionSet {
    kernel: Arc<VersionSetKernel>,
    column_family_set: Vec<Option<ColumnFamily>>,
    comparator: InternalKeyComparator,
}

impl VersionSet {
    pub async fn recover(mut reader: Box<LogReader>, options: &ImmutableDBOptions) -> Result<Self> {
        let mut record = vec![];
        let mut cfs: HashMap<u32, ColumnFamily> = HashMap::default();
        let mut edits: HashMap<u32, Vec<VersionEdit>> = HashMap::default();
        let kernel = Arc::new(VersionSetKernel::default());
        while reader.read_record(&mut record).await? {
            let mut edit = VersionEdit::default();
            edit.decode_from(&record)?;
            if edit.is_column_family_add {
                edits.insert(edit.column_family, vec![]);
                cfs.insert(
                    edit.column_family,
                    ColumnFamily::new(
                        edit.column_family as usize,
                        edit.column_family_name.clone(),
                        Memtable::new(kernel.new_memtable_number(), options.comparator.clone()),
                        options.comparator.clone(),
                        Arc::new(Version::new(
                            edit.column_family,
                            edit.column_family_name.clone(),
                            vec![],
                        )),
                    ),
                );
            } else if edit.is_column_family_drop {
                edits.remove(&edit.column_family);
                cfs.remove(&edit.column_family);
            } else {
                if let Some(data) = edits.get_mut(&edit.column_family) {
                    data.push(edit);
                }
            }
        }
        let mut column_family_set = vec![];
        for (cf_id, edit) in edits {
            if let Some(mut cf) = cfs.remove(&cf_id) {
                let version = cf.get_version();
                let new_version = version.apply(edit);
                cf.install_version(vec![], new_version);
                while cf_id as usize >= column_family_set.len() {
                    column_family_set.push(None);
                }
                column_family_set[cf_id as usize] = Some(cf);
            }
        }
        Ok(VersionSet {
            kernel,
            column_family_set,
            comparator: options.comparator.clone(),
        })
    }

    pub fn new_file_number(&self) -> u64 {
        self.kernel.new_file_number()
    }

    pub fn should_flush(&self) -> bool {
        for cf in self.column_family_set.iter() {
            if cf.as_ref().map_or(false, |cf| cf.should_flush()) {
                return true;
            }
        }
        false
    }

    pub fn get_column_family_versions(&self) -> Vec<Arc<Version>> {
        let mut versions = vec![];
        for c in self.column_family_set.iter() {
            if let Some(cf) = c {
                versions.push(cf.get_version())
            }
        }
        versions
    }

    pub fn get_comparator_name(&self) -> &str {
        self.comparator.name()
    }

    pub fn create_column_family(&mut self, edit: VersionEdit) -> Result<Arc<Version>> {
        let id = edit.column_family as usize;
        let name = edit.column_family_name.clone();
        let m = Memtable::new(self.kernel.new_memtable_number(), self.comparator.clone());
        let log_number = edit.log_number;
        let new_version = Arc::new(Version::new(edit.column_family, name.clone(), vec![edit]));
        let mut cf = ColumnFamily::new(id, name, m, self.comparator.clone(), new_version.clone());
        cf.set_log_number(log_number);
        while self.column_family_set.len() <= id {
            self.column_family_set.push(None);
        }
        self.column_family_set[id] = Some(cf);
        Ok(new_version)
    }

    pub fn install_version(
        &mut self,
        cf_id: u32,
        mems: Vec<u64>,
        version: Version,
    ) -> Result<Arc<Version>> {
        let pos = cf_id as usize;
        if pos > self.column_family_set.len() {
            Err(Error::CompactionError(format!(
                "column faimly has not been created"
            )))
        } else {
            if let Some(cf) = self.column_family_set[cf_id as usize].as_mut() {
                Ok(cf.install_version(mems, version))
            } else {
                Err(Error::CompactionError(format!(
                    "column faimly has been dropped"
                )))
            }
        }
    }
}
