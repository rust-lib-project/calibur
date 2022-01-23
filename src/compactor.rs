use crate::db::Core;
use std::sync::{Arc, Mutex};

pub struct Compactor {
    core: Arc<Mutex<Core>>,
}
