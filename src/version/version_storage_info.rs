use std::sync::Arc;

use crate::table::SortedStringTable;

pub struct VersionStorageInfo {
    level0: Vec<Arc<dyn SortedStringTable>>,
    base_level: Vec<Vec<Arc<dyn SortedStringTable>>>,
}
