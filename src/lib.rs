#![allow(dead_code)]

mod common;
mod compaction;
mod db;
mod memtable;
mod table;
mod version;
mod wal;
mod write_batch;

pub use db::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
