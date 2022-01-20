use std::sync::Arc;

use crate::table::TableReader;

pub struct VersionStorageInfo {
    level0: Vec<Arc<dyn TableReader>>,
    base_level: Vec<Vec<Arc<dyn TableReader>>>,
}
