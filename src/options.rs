use crate::common::FileSystem;
use crate::table::TableFactory;
use std::sync::Arc;

pub struct ImmutableDBOptions {
    pub max_manifest_file_size: usize,
    pub db_path: String,
    pub fs: Arc<dyn FileSystem>,
    pub factory: Arc<dyn TableFactory>,
}
