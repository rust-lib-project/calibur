use crate::log::LogWriter;
use crate::version::KernelNumberContext;
use std::sync::Arc;

pub struct WALWriter {
    kernel: Arc<KernelNumberContext>,
    writer: Box<LogWriter>,
    logs: Vec<Box<LogWriter>>,
    last_sequence: u64,
}
