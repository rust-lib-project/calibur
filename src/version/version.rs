use crate::version::version_storage_info::VersionStorageInfo;
use crate::version::TableFile;
use std::sync::Arc;

pub struct Version {
    cf_id: u32,
    log_number: u64,
    cf_name: String,
    comparator: String,
    storage: VersionStorageInfo,
}

impl Version {
    pub fn new(
        cf_id: u32,
        cf_name: String,
        comparator: String,
        tables: Vec<Arc<TableFile>>,
        log_number: u64,
        max_level: u32,
    ) -> Self {
        Version {
            storage: VersionStorageInfo::new(tables, max_level as usize),
            cf_id,
            cf_name,
            log_number,
            comparator,
        }
    }

    pub fn apply(
        &self,
        to_add: Vec<Arc<TableFile>>,
        to_delete: Vec<Arc<TableFile>>,
        log_number: u64,
    ) -> Self {
        let storage = self.storage.apply(to_add, to_delete);
        Version {
            storage,
            cf_id: self.cf_id,
            cf_name: self.cf_name.clone(),
            log_number: std::cmp::max(self.log_number, log_number),
            comparator: self.comparator.clone(),
        }
    }

    pub fn get_log_number(&self) -> u64 {
        self.log_number
    }

    pub fn get_cf_id(&self) -> u32 {
        self.cf_id
    }

    pub fn get_cf_name(&self) -> &str {
        &self.cf_name
    }

    pub fn get_comparator_name(&self) -> &str {
        &self.comparator
    }

    pub fn get_level_num(&self) -> usize {
        self.storage.size()
    }

    pub fn scan<F: FnMut(&Arc<TableFile>)>(&self, f: F, level: usize) {
        self.storage.scan(f, level);
    }

    pub fn get_storage_info(&self) -> &VersionStorageInfo {
        &self.storage
    }

    pub fn update_base_bytes(
        &mut self,
        max_bytes_for_level_base: u64,
        level0_file_num_compaction_trigger: usize,
        max_bytes_for_level_multiplier: f64,
    ) {
        self.storage.update_base_bytes(
            max_bytes_for_level_base,
            level0_file_num_compaction_trigger,
            max_bytes_for_level_multiplier,
        );
    }
}
