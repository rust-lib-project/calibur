use crate::common::{FileSystem, Result};
use crate::log::LogWriter;
use crate::options::ImmutableDBOptions;
use crate::version::{Version, VersionEdit, VersionSet};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Manifest {
    log: Box<LogWriter>,
    fs: Arc<dyn FileSystem>,
    versions: HashMap<u32, Arc<Version>>,
    version_set: Arc<Mutex<VersionSet>>,
    options: Arc<ImmutableDBOptions>,
}

impl Manifest {
    pub async fn process_manifest_writes(&mut self, edits: Vec<VersionEdit>) -> Result<()> {
        if self.log.get_file_size() > self.options.max_manifest_file_size {
            // TODO: Switch manifest log writer
        }
        let mut data = vec![];
        for e in &edits {
            e.encode_to(&mut data);
            self.log.add_record(&data).await?;
            data.clear();
        }
        let mut versions = HashMap::<u32, Vec<VersionEdit>>::new();
        for e in edits {
            match versions.get_mut(&e.column_family) {
                Some(v) => {
                    v.push(e);
                }
                None => {
                    let cf = e.column_family;
                    let v = vec![e];
                    versions.insert(cf, v);
                }
            }
        }
        for (cf, mut edits) in versions {
            // TODO: apply create and drop column family
            let version = self.versions.get(&cf).unwrap();
            let mut mems = vec![];
            for e in &mut edits {
                mems.append(&mut e.mems_deleted)
            }
            let new_version = version.apply(edits);
            let mut version_set = self.version_set.lock().unwrap();
            version_set.install_version(cf, mems, new_version);
        }
        self.log.fsync().await?;
        Ok(())
    }
}
