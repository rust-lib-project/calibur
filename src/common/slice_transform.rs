pub trait SliceTransform: Send + Sync {
    fn name(&self) -> &'static str;
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8];
    fn in_domain(&self, key: &[u8]) -> bool;
    fn in_range(&self, key: &[u8]) -> bool {
        false
    }
}
