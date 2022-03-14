use crate::common::{FileSystem, InternalKeyComparator, InternalKeySliceTransform};
use crate::memtable::MemTableContext;
use crate::table::{BlockBasedTableFactory, TableFactory};
use crate::{KeyComparator, SliceTransform, SyncPosixFileSystem};
use std::sync::Arc;

pub struct ImmutableDBOptions {
    pub max_manifest_file_size: usize,
    pub max_total_wal_size: usize,
    pub db_path: String,
    pub fs: Arc<dyn FileSystem>,
    pub max_background_jobs: usize,
}

#[derive(Clone)]
pub struct ColumnFamilyOptions {
    pub write_buffer_size: usize,
    pub max_write_buffer_number: usize,
    pub factory: Arc<dyn TableFactory>,
    pub comparator: InternalKeyComparator,
    pub prefix_extractor: Arc<dyn SliceTransform>,
    pub max_level: u32,
    pub max_bytes_for_level_base: u64,
    pub max_bytes_for_level_multiplier: f64,
    pub target_file_size_base: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_compaction_bytes: usize,
}

impl PartialEq for ColumnFamilyOptions {
    fn eq(&self, other: &Self) -> bool {
        self.write_buffer_size == other.write_buffer_size
            && self.max_write_buffer_number == other.max_write_buffer_number
            && self.factory.name().eq(other.factory.name())
            && self.comparator.name().eq(other.comparator.name())
            && self.max_level == other.max_level
            && self.max_bytes_for_level_base == other.max_bytes_for_level_base
            && self.max_bytes_for_level_multiplier == other.max_bytes_for_level_multiplier
            && self.target_file_size_base == other.target_file_size_base
    }
}

impl Eq for ColumnFamilyOptions {}

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        ColumnFamilyOptions {
            write_buffer_size: 4 << 20,
            max_write_buffer_number: 1,
            factory: Arc::new(BlockBasedTableFactory::default()),
            comparator: InternalKeyComparator::default(),
            prefix_extractor: Arc::new(InternalKeySliceTransform::default()),
            max_level: 7,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10.0,
            target_file_size_base: 64 * 1024 * 1024,
            level0_file_num_compaction_trigger: 4,
            max_compaction_bytes: 64 * 1024usize * 1024usize * 25,
        }
    }
}

#[derive(Clone)]
pub struct DBOptions {
    pub max_manifest_file_size: usize,
    pub max_total_wal_size: usize,
    pub create_if_missing: bool,
    pub create_missing_column_families: bool,
    pub fs: Arc<dyn FileSystem>,
    pub db_path: String,
    pub db_name: String,
    pub max_background_jobs: usize,
}

impl Default for DBOptions {
    fn default() -> Self {
        Self {
            max_manifest_file_size: 128 * 1024 * 1024, // 100MB
            max_total_wal_size: 128 * 1024 * 1024,     // 100MB
            create_if_missing: false,
            create_missing_column_families: false,
            fs: Arc::new(SyncPosixFileSystem {}),
            db_path: "db".to_string(),
            db_name: "db".to_string(),
            max_background_jobs: 2,
        }
    }
}

#[derive(Clone)]
pub struct ColumnFamilyDescriptor {
    pub name: String,
    pub options: ColumnFamilyOptions,
}

impl From<DBOptions> for ImmutableDBOptions {
    fn from(opt: DBOptions) -> Self {
        Self {
            max_manifest_file_size: opt.max_manifest_file_size,
            max_total_wal_size: opt.max_total_wal_size,
            db_path: opt.db_path.clone(),
            fs: opt.fs.clone(),
            max_background_jobs: opt.max_background_jobs,
        }
    }
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    pub snapshot: Option<u64>,
    pub fill_cache: bool,
    pub total_order_seek: bool,
    pub prefix_same_as_start: bool,
    pub skip_filter: bool,
}

#[derive(Default, Clone)]
pub struct WriteOptions {
    pub ctx: MemTableContext,
    pub disable_wal: bool,
    pub sync: bool,
}
