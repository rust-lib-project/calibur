#![allow(dead_code)]

mod column_family;
mod common;
mod compaction;
mod db;
mod log;
mod manifest;
mod memtable;
mod options;
mod table;
mod util;
mod version;
mod wal;
mod write_batch;

pub use common::{
    Error, FileSystem, InternalKeyComparator, KeyComparator, Result, SliceTransform,
    SyncPoxisFileSystem,
};
pub use db::*;
pub use options::*;
pub use table::{
    BlockBasedTableFactory, BlockBasedTableOptions, FilterBlockFactory, FullFilterBlockFactory,
};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
