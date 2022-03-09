use crate::Splice;

#[derive(Default, Clone)]
pub struct MemTableContext {
    pub(crate) splice: Splice,
    // TODO: Support allocate data from local thread arena.
}
