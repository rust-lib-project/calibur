use crate::common::{
    make_current_file, make_descriptor_file_name, parse_file_name, DBFileType, Error, FileSystem,
    InternalKeyComparator, Result,
};
use crate::memtable::Memtable;
use crate::table::{InternalIterator, MergingIterator};
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};

use crate::compaction::CompactionEngine;
use crate::log::{LogReader, LogWriter};
use crate::options::{ColumnFamilyDescriptor, ImmutableDBOptions};
use crate::util::BtreeComparable;
use crate::version::{KernelNumberContext, Version, VersionEdit, VersionSet};
use futures::SinkExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct Manifest {
    log: Option<Box<LogWriter>>,
    versions: HashMap<u32, Arc<Version>>,
    version_set: Arc<Mutex<VersionSet>>,
    kernel: Arc<KernelNumberContext>,
    options: Arc<ImmutableDBOptions>,
    manifest_file_number: u64,
}

impl Manifest {
    pub async fn recover(
        cfs: &[ColumnFamilyDescriptor],
        db_options: &Arc<ImmutableDBOptions>,
    ) -> Result<Self> {
        let (manifest_path, manifest_file_number) =
            get_current_manifest_path(&db_options.db_path, db_options.fs.clone()).await?;
        let reader = db_options
            .fs
            .open_sequencial_file(PathBuf::from(manifest_path))?;
        let log_reader = LogReader::new(reader);
        let version_set = VersionSet::read_recover(cfs, Box::new(log_reader), db_options).await?;
        let cf_versions = version_set.get_column_family_versions();
        let mut versions = HashMap::default();
        let kernal = version_set.get_kernel();
        for v in cf_versions {
            versions.insert(v.get_cf_id(), v);
        }
        Ok(Self {
            log: None,
            version_set: Arc::new(Mutex::new(version_set)),
            manifest_file_number,
            versions,
            kernel: kernal,
            options: db_options.clone(),
        })
    }

    pub fn get_version_set(&self) -> Arc<Mutex<VersionSet>> {
        self.version_set.clone()
    }

    pub async fn process_manifest_writes(&mut self, edits: Vec<VersionEdit>) -> Result<()> {
        if self.log.as_ref().map_or(true, |f| {
            f.get_file_size() > self.options.max_manifest_file_size
        }) {
            // TODO: Switch manifest log writer
            let file_number = self.kernel.new_file_number();
            let descrip_file_name = make_descriptor_file_name(&self.options.db_path, file_number);
            let writer = self.options.fs.open_writable_file(descrip_file_name)?;
            let mut writer = LogWriter::new(writer, 0);
            self.write_snapshot(&mut writer).await?;
            self.log = Some(Box::new(writer));
            self.manifest_file_number = file_number;
        }
        let mut data = vec![];
        for e in &edits {
            e.encode_to(&mut data);
            self.log.as_mut().unwrap().add_record(&data).await?;
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
            if edits.len() == 1 && edits[0].is_column_family_add {
                let mut version_set = self.version_set.lock().unwrap();
                let version = version_set.create_column_family(edits.pop().unwrap())?;
                self.versions.insert(cf, version);
                continue;
            }
            let version = self.versions.get(&cf).unwrap();
            let mut mems = vec![];
            for e in &mut edits {
                mems.append(&mut e.mems_deleted)
            }
            // Do not apply version edits with holding a mutex.
            let new_version = version.apply(edits);
            let mut version_set = self.version_set.lock().unwrap();
            let version = version_set.install_version(cf, mems, new_version)?;
            self.versions.insert(cf, version);
        }
        self.log.as_mut().unwrap().fsync().await?;
        Ok(())
    }

    async fn write_snapshot(&mut self, writer: &mut LogWriter) -> Result<()> {
        let (versions, comparator) = {
            let version_set = self.version_set.lock().unwrap();
            (
                version_set.get_column_family_versions(),
                version_set.get_comparator_name().to_string(),
            )
        };
        for version in versions {
            let mut record = Vec::new();
            {
                let mut edit = VersionEdit::default();
                edit.column_family_name = version.get_cf_name().to_string();
                edit.column_family = version.get_cf_id() as u32;
                edit.is_column_family_add = true;
                edit.comparator = comparator.clone();
                if !edit.encode_to(&mut record) {
                    return Err(Error::CompactionError(format!(
                        "write snapshot failed because encode failed"
                    )));
                }
                writer.add_record(&record).await?;
                record.clear();
            }
            let mut edit = VersionEdit::default();
            edit.column_family = version.get_cf_id() as u32;
            edit.log_number = version.get_log_number();
            let levels = version.get_level_num();
            for i in 0..=levels {
                version.scan(
                    |f| {
                        edit.add_file(
                            i as u32,
                            f.id(),
                            f.fd.file_size,
                            f.smallest.as_ref(),
                            f.largest.as_ref(),
                            f.fd.smallest_seqno,
                            f.fd.largest_seqno,
                        );
                    },
                    i,
                );
            }
            edit.encode_to(&mut record);
            writer.add_record(&record).await?;
        }
        writer.fsync().await?;
        Ok(())
    }
}

pub struct ManifestWriter {
    manifest: Box<Manifest>,
    cbs: Vec<OnceSender<Result<()>>>,
    edits: Vec<VersionEdit>,
}

impl ManifestWriter {
    pub fn new(manifest: Box<Manifest>) -> Self {
        Self {
            manifest,
            cbs: vec![],
            edits: vec![],
        }
    }

    pub fn batch(&mut self, mut task: ManifestTask) {
        self.edits.append(&mut task.edits);
        self.cbs.push(task.cb);
    }

    pub async fn apply(&mut self) {
        let edits = std::mem::take(&mut self.edits);
        match self.manifest.process_manifest_writes(edits).await {
            Ok(()) => {
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Ok(()));
                }
            }
            Err(e) => {
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Err(e.clone()));
                }
            }
        }
    }
}

pub struct ManifestTask {
    edits: Vec<VersionEdit>,
    cb: OnceSender<Result<()>>,
}

#[derive(Clone)]
pub struct ManifestEngine {
    sender: UnboundedSender<ManifestTask>,
    comparator: InternalKeyComparator,
}

impl ManifestEngine {
    pub fn new(sender: UnboundedSender<ManifestTask>, comparator: InternalKeyComparator) -> Self {
        Self { sender, comparator }
    }
}

#[async_trait::async_trait]
impl CompactionEngine for ManifestEngine {
    async fn apply(&mut self, edits: Vec<VersionEdit>) -> Result<()> {
        let (cb, rx) = once_channel();
        let task = ManifestTask { edits, cb };
        // let mut sender = self.sender.clone();
        self.sender
            .send(task)
            .await
            .map_err(|e| Error::Cancel(format!("the manifest thread may close, {:?}", e)))?;
        let x = rx.await.map_err(|e| {
            Error::Cancel(format!(
                "The manifest thread may cancel this apply, {:?}",
                e
            ))
        })?;
        x
    }

    fn new_merging_iterator(&self, mems: &[Arc<Memtable>]) -> Box<dyn InternalIterator> {
        if mems.len() == 1 {
            return mems[0].new_iterator();
        } else {
            let iters = mems.iter().map(|mem| mem.new_iterator()).collect();
            let iter = MergingIterator::new(iters, self.comparator.clone());
            Box::new(iter)
        }
    }
}

pub async fn get_current_manifest_path(
    dbname: &String,
    fs: Arc<dyn FileSystem>,
) -> Result<(String, u64)> {
    let mut data = fs
        .read_file_content(make_current_file(dbname.as_str()))
        .await?;
    if data.is_empty() || *data.last().unwrap() != b'\n' {
        return Err(Error::InvalidFile(format!("CURRENT file corrupted")));
    }
    data.pop();
    let fname = String::from_utf8(data).map_err(|e| {
        Error::InvalidFile(format!(
            "can not trans current file to manifest name, Error: {:?}",
            e
        ))
    })?;
    let (tp, manifest_file_number) = parse_file_name(&fname)?;
    if tp != DBFileType::DescriptorFile {
        return Err(Error::InvalidFile(format!("CURRENT file corrupted")));
    }
    let mut manifest_path = dbname.clone();
    if !manifest_path.ends_with("/") {
        manifest_path.push('/');
    }
    manifest_path.push_str(&fname);
    Ok((manifest_path, manifest_file_number))
}
