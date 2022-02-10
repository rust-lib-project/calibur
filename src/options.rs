use crate::common::{FileSystem, InternalKeyComparator};
use crate::table::TableFactory;
use std::sync::Arc;

pub struct ImmutableDBOptions {
    pub comparator: InternalKeyComparator,
    pub max_manifest_file_size: usize,
    pub db_path: String,
    pub fs: Arc<dyn FileSystem>,
    pub factory: Arc<dyn TableFactory>,
}

pub struct ColumnFamilyOptions {}

pub struct DBOptions {}

pub struct ColumnFamilyDescriptor {
    pub name: String,
    pub options: ColumnFamilyOptions,
}
