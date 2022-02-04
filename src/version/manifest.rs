use std::sync::Arc;
use crate::common::FileSystem;
use crate::log::LogWriter;

pub struct Manifest {
    log: Box<LogWriter>,
    fs: Arc<dyn FileSystem>,

}