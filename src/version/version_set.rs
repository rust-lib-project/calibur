use crate::common::{Error, FileSystem, KeyComparator, Result};
use crate::memtable::Memtable;
use crate::options::{ColumnFamilyDescriptor, ColumnFamilyOptions};
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
    column_family_set: HashMap<u32, ColumnFamily>,
    fs: Arc<dyn FileSystem>,
}

impl VersionSet {
    pub fn new(
        cf_descriptor: &[ColumnFamilyDescriptor],
        kernel: Arc<KernelNumberContext>,
        fs: Arc<dyn FileSystem>,
        versions: HashMap<u32, Arc<Version>>,
    ) -> Self {
        let mut cf_options: HashMap<String, ColumnFamilyOptions> = HashMap::default();
        for cf in cf_descriptor.iter() {
            cf_options.insert(cf.name.clone(), cf.options.clone());
        }
        let mut column_family_set = HashMap::default();
        for (cf, version) in versions {
            let cf_opt = cf_options
                .remove(version.get_cf_name())
                .unwrap_or(ColumnFamilyOptions::default());
            column_family_set.insert(
                cf,
                ColumnFamily::new(
                    cf,
                    version.get_cf_name().to_string(),
                    Memtable::new(kernel.new_memtable_number(), cf_opt.comparator.clone()),
                    cf_opt.comparator.clone(),
                    version,
                    cf_opt,
                ),
            );
        }
        VersionSet {
            kernel,
            column_family_set,
            fs,
        }
    }

    pub fn get_kernel(&self) -> Arc<KernelNumberContext> {
        self.kernel.clone()
    }

    pub fn new_file_number(&self) -> u64 {
        self.kernel.new_file_number()
    }

    pub fn should_flush(&self) -> bool {
        for (_, cf) in self.column_family_set.iter() {
            if cf.should_flush() {
                return true;
            }
        }
        false
    }

    pub fn get_column_family_versions(&self) -> Vec<Arc<Version>> {
        let mut versions = vec![];
        for (_, cf) in self.column_family_set.iter() {
            versions.push(cf.get_version())
        }
        versions
    }

    pub fn get_superversion(&self, cf_id: u32) -> Option<Arc<SuperVersion>> {
        if let Some(cf) = self.column_family_set.get(&cf_id) {
            return Some(cf.get_super_version());
        }
        None
    }

    pub fn get_column_family_options(&self) -> Vec<(u32, Arc<ColumnFamilyOptions>)> {
        let mut options = vec![];
        for (&cf_id, cf) in self.column_family_set.iter() {
            options.push((cf_id, cf.get_options()));
        }
        options
    }

    pub fn create_column_family(&mut self, mut edit: VersionEdit) -> Result<Arc<Version>> {
        let cf_opt = edit.cf_options.options.take().unwrap();
        let id = edit.column_family;
        let name = edit.column_family_name.clone();
        let m = Memtable::new(self.kernel.new_memtable_number(), cf_opt.comparator.clone());
        let log_number = edit.log_number;
        let new_version = Arc::new(Version::new(
            edit.column_family,
            name.clone(),
            cf_opt.comparator.name().to_string(),
            vec![],
            edit.log_number,
        ));
        let mut cf = ColumnFamily::new(
            id,
            name,
            m,
            cf_opt.comparator.clone(),
            new_version.clone(),
            cf_opt,
        );
        cf.set_log_number(log_number);
        self.column_family_set.insert(id, cf);
        Ok(new_version)
    }

    pub fn install_version(
        &mut self,
        cf_id: u32,
        mems: Vec<u64>,
        version: Version,
    ) -> Result<Arc<Version>> {
        if let Some(cf) = self.column_family_set.get_mut(&cf_id) {
            Ok(cf.install_version(mems, version))
        } else {
            Err(Error::CompactionError(format!(
                "column faimly has been dropped"
            )))
        }
    }

    // TODO: record error
    pub fn record_error(&mut self, _: Error) {}
}
