use crate::version::VersionEdit;

pub trait CompactionEngine: Clone + Sync + Send {
    fn apply(&self, version: Vec<VersionEdit>);
}

pub struct CompactionJob<C: CompactionEngine> {
    engine: C,
}
