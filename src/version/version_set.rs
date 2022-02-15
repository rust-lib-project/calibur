use crate::common::{Error, FileSystem, InternalKeyComparator, KeyComparator, Result};
use crate::log::LogReader;
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyDescriptor, ColumnFamilyOptions, ImmutableDBOptions};
use crate::version::column_family::ColumnFamily;
use crate::version::{SuperVersion, Version, VersionEdit};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{atomic, Arc};

#[derive(Default)]
pub struct KernelNumberContext {
    next_file_number: atomic::AtomicU64,
    next_mem_number: atomic::AtomicU64,
    last_sequence: atomic::AtomicU64,
}

impl KernelNumberContext {
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
        while old <= v {
            match self.next_file_number.compare_exchange(
                old,
                v + 1,
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
    kernel: Arc<KernelNumberContext>,
    column_family_set: Vec<Option<ColumnFamily>>,
    comparator: InternalKeyComparator,
    fs: Arc<dyn FileSystem>,
}

impl VersionSet {
    pub async fn read_recover(
        cf_descriptor: &[ColumnFamilyDescriptor],
        mut reader: Box<LogReader>,
        options: &ImmutableDBOptions,
    ) -> Result<Self> {
        // let cf_options = cfs.iter().map(|d|d.options.clone()).collect();
        let mut record = vec![];
        let mut cfs: HashMap<u32, ColumnFamily> = HashMap::default();
        let mut edits: HashMap<u32, Vec<VersionEdit>> = HashMap::default();
        let kernel = Arc::new(KernelNumberContext::default());
        let mut cf_options: HashMap<String, ColumnFamilyOptions> = HashMap::default();
        for cf in cf_descriptor.iter() {
            cf_options.insert(cf.name.clone(), cf.options.clone());
        }
        while reader.read_record(&mut record).await? {
            let mut edit = VersionEdit::default();
            edit.decode_from(&record)?;
            if edit.is_column_family_add {
                edits.insert(edit.column_family, vec![]);
                let cf_opt = cf_options
                    .get(&edit.column_family_name)
                    .cloned()
                    .unwrap_or(ColumnFamilyOptions::default());
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
                        cf_opt,
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
        let mut has_prev_log_number = false;
        let mut prev_log_number = 0;
        let mut has_next_file_number = false;
        let mut next_file_number = 0;
        let mut has_last_sequence = false;
        let mut last_sequence = 0;
        for (cf_id, edit) in edits {
            if let Some(mut cf) = cfs.remove(&cf_id) {
                let version = cf.get_version();
                for e in &edit {
                    if e.has_prev_log_number {
                        has_prev_log_number = true;
                        prev_log_number = e.prev_log_number;
                    }
                    if e.has_next_file_number {
                        has_next_file_number = true;
                        next_file_number = e.next_file_number;
                    }
                    if e.has_last_sequence {
                        has_last_sequence = true;
                        last_sequence = e.last_sequence;
                    }
                }
                let new_version = version.apply(edit);
                cf.install_version(vec![], new_version);
                while cf_id as usize >= column_family_set.len() {
                    column_family_set.push(None);
                }
                column_family_set[cf_id as usize] = Some(cf);
            }
        }
        if has_prev_log_number {
            kernel.mark_file_number_used(prev_log_number);
        }
        if has_next_file_number {
            kernel.mark_file_number_used(next_file_number);
        }
        if has_last_sequence {
            kernel.last_sequence.store(last_sequence, Ordering::Release);
        }
        Ok(VersionSet {
            kernel,
            column_family_set,
            comparator: options.comparator.clone(),
            fs: options.fs.clone(),
        })
    }

    pub fn get_kernel(&self) -> Arc<KernelNumberContext> {
        self.kernel.clone()
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
    pub fn get_superversion(&self, cf_id: usize) -> Option<Arc<SuperVersion>> {
        if cf_id < self.column_family_set.len() {
            if let Some(cf) = self.column_family_set[cf_id].as_ref() {
                return Some(cf.get_super_version());
            }
        }
        None
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
        let mut cf = ColumnFamily::new(
            id,
            name,
            m,
            self.comparator.clone(),
            new_version.clone(),
            ColumnFamilyOptions::default(),
        );
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
