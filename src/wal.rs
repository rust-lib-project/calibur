use crate::common::{Result, WritableFile};
use std::path::PathBuf;

pub const MAX_HEADER_SIZE: usize = 21;

/// WAL of a memtable or a value log
///
/// TODO: This WAL simply stores key-value pair in sequence without checksum,
/// encryption and compression. These will be done later.
/// TODO: delete WAL file when reference to WAL (or memtable) comes to 0
pub struct Wal {
    path: PathBuf,
    file: Box<dyn WritableFile>,
    size: u32,
}

impl Wal {
    /// open or create a WAL from options
    pub fn open(path: PathBuf) -> Result<Wal> {
        unimplemented!()
    }

    pub fn sync(&mut self) -> Result<()> {
        unimplemented!()
    }
}

pub struct WalManager {}

pub struct LogWriter {}
