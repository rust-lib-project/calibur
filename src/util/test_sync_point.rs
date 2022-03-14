use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

type Runner = Arc<dyn Fn(Option<String>) -> Option<String> + Send + Sync>;
type Callback = Box<dyn FnOnce(Option<String>) -> Option<String> + Send>;

#[derive(Default)]
struct SyncPointRegistry {
    // TODO: remove rwlock or store *mut FailPoint
    registry: RwLock<HashMap<String, Runner>>,
    once: Arc<Mutex<HashMap<String, Callback>>>,
}

lazy_static::lazy_static! {
    static ref REGISTRY: SyncPointRegistry = SyncPointRegistry::default();
    static ref ENABLE: AtomicBool = AtomicBool::new(false);
}

#[macro_export]
#[cfg(test)]
macro_rules! sync_point {
    ($name:expr) => {{
        crate::util::callback_for_point($name, None);
    }};
    ($name:expr, $e:expr) => {{
        let args = format!("{:?}", $e);
        let _ = crate::util::callback_for_point($name, Some(args));
    }};
}

#[macro_export]
#[cfg(test)]
macro_rules! sync_point_return {
    ($name:expr, $f:expr) => {{
        if let Some(v) = crate::util::callback_for_point($name, None) {
            return $f(v);
        }
    }};
}

#[macro_export]
#[cfg(not(test))]
macro_rules! sync_point {
    ($name:expr) => {{}};
    ($name:expr, $e:expr) => {{}};
}

#[macro_export]
#[cfg(not(test))]
macro_rules! sync_point_return {
    ($name:expr, $e:expr) => {{}};
}

pub fn callback_for_point(name: &str, args: Option<String>) -> Option<String> {
    if !ENABLE.load(Ordering::Acquire) {
        return None;
    }
    if let Some(f) = {
        let mut registry = REGISTRY.once.lock().unwrap();
        registry.remove(name)
    } {
        return f(args);
    }
    let p = {
        let registry = REGISTRY.registry.read().unwrap();
        registry.get(name).cloned()
    };
    if let Some(f) = p {
        return f(args);
    }
    None
}

pub fn enable_processing() {
    ENABLE.store(true, Ordering::Release);
}

pub fn disable_processing() {
    ENABLE.store(false, Ordering::Release);
}

pub fn clear_all_callbacks() {
    let mut registry = REGISTRY.registry.write().unwrap();
    registry.clear();
}

pub fn set_callback<F: Fn(Option<String>) -> Option<String> + Send + Sync + 'static>(
    name: &str,
    f: F,
) {
    let cb = Arc::new(f);
    let mut registry = REGISTRY.registry.write().unwrap();
    registry.insert(name.to_string(), cb);
}

pub fn set_once_callback<F: FnOnce(Option<String>) -> Option<String> + Send + 'static>(
    name: &str,
    f: F,
) {
    let cb = Box::new(f);
    let mut registry = REGISTRY.once.lock().unwrap();
    registry.insert(name.to_string(), cb);
}
