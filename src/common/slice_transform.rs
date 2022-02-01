use crate::util::extract_user_key;
use std::sync::Arc;

pub trait SliceTransform: Send + Sync {
    fn name(&self) -> &'static str;
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8];
    fn in_domain(&self, key: &[u8]) -> bool;
    fn in_range(&self, _key: &[u8]) -> bool {
        false
    }
}

pub struct InternalKeySliceTransform {
    transform: Arc<dyn SliceTransform>,
}

impl InternalKeySliceTransform {
    pub fn new(transform: Arc<dyn SliceTransform>) -> Self {
        Self { transform }
    }

    pub fn user_prefix_extractor(&self) -> Arc<dyn SliceTransform> {
        self.transform.clone()
    }
}

impl SliceTransform for InternalKeySliceTransform {
    fn name(&self) -> &'static str {
        self.transform.name()
    }

    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        self.transform.transform(extract_user_key(key))
    }

    fn in_domain(&self, key: &[u8]) -> bool {
        self.transform.in_domain(extract_user_key(key))
    }

    fn in_range(&self, key: &[u8]) -> bool {
        self.transform.in_range(extract_user_key(key))
    }
}
