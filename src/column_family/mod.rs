use std::sync::{Arc, Mutex, atomic::AtomicU64};
use crate::version::{MemtableList, SuperVersion};
use crate::memtable::Memtable;


pub struct ColumnFamily {
    mem: Arc<Memtable>,
    imms: Arc<MemtableList>,
    super_version: Arc<SuperVersion>,

    // An ordinal representing the current SuperVersion. Updated by
    // InstallSuperVersion(), i.e. incremented every time super_version_
    // changes.
    super_version_number: AtomicU64,
}