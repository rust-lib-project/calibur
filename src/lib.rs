#![allow(dead_code)]

mod column_family;
mod common;
mod compaction;
mod compactor;
mod db;
mod memtable;
mod table;
mod util;
mod version;
mod wal;
mod write_batch;
mod write_thread;

pub use db::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
