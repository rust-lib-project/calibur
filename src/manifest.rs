use crate::common::{make_descriptor_file_name, Error, InternalKeyComparator, Result};
use crate::memtable::Memtable;
use crate::table::{InternalIterator, MergingIterator};
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};

use crate::compaction::CompactionEngine;
use crate::log::{LogReader, LogWriter};
use crate::options::ImmutableDBOptions;
use crate::util::BtreeComparable;
use crate::version::{Version, VersionEdit, VersionSet, VersionSetKernel};
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Manifest {
    log: Box<LogWriter>,
    versions: HashMap<u32, Arc<Version>>,
    version_set: Arc<Mutex<VersionSet>>,
    kernal: Arc<VersionSetKernel>,
    options: Arc<ImmutableDBOptions>,
    manifest_file_number: u64,
}

impl Manifest {
    pub async fn recover(mut reader: Box<LogReader>) -> Result<HashMap<u32, Arc<Version>>> {
        let mut record = vec![];
        let versions = HashMap::default();
        while reader.read_record(&mut record).await? {
            let mut edit = VersionEdit::default();
            edit.decode_from(&record)?;
        }
        Ok(versions)
    }

    pub async fn process_manifest_writes(&mut self, edits: Vec<VersionEdit>) -> Result<()> {
        if self.log.get_file_size() > self.options.max_manifest_file_size {
            // TODO: Switch manifest log writer
            let file_number = self.kernal.new_file_number();
            let descrip_file_name = make_descriptor_file_name(&self.options.db_path, file_number);
            let writer = self.options.fs.open_writable_file(descrip_file_name)?;
            let mut writer = LogWriter::new(writer, 0);
            self.write_snapshot(&mut writer).await?;
            self.log = Box::new(writer);
            self.manifest_file_number = file_number;
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
            let new_version = version.apply(edits);
            let mut version_set = self.version_set.lock().unwrap();
            let version = version_set.install_version(cf, mems, new_version)?;
            self.versions.insert(cf, version);
        }
        self.log.fsync().await?;
        Ok(())
    }

    async fn write_snapshot(&mut self, writer: &mut LogWriter) -> Result<()> {
        let (cfs, comparator) = {
            let version_set = self.version_set.lock().unwrap();
            (
                version_set.get_column_familys(),
                version_set.get_comparator_name().to_string(),
            )
        };
        for cf in cfs {
            let mut record = Vec::new();
            {
                let mut edit = VersionEdit::default();
                edit.column_family_name = cf.get_name().to_string();
                edit.column_family = cf.get_id() as u32;
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
            edit.column_family = cf.get_id() as u32;
            edit.log_number = cf.get_log_number();
            let version = cf.get_version();
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
            writer.fsync().await?;
        }
        Ok(())
    }
}

pub struct ManifestWriter {
    manifest: Box<Manifest>,
    cbs: Vec<OnceSender<Result<()>>>,
    edits: Vec<VersionEdit>,
}

impl ManifestWriter {
    pub fn new(
        versions: HashMap<u32, Arc<Version>>,
        version_set: Arc<Mutex<VersionSet>>,
        kernel: Arc<VersionSetKernel>,
        options: Arc<ImmutableDBOptions>,
    ) -> Self {
        let manifest_file_number = kernel.new_file_number();
        let descrip_file_name = make_descriptor_file_name(&options.db_path, manifest_file_number);
        let writer = options.fs.open_writable_file(descrip_file_name).unwrap();
        let manifest = Box::new(Manifest {
            log: Box::new(LogWriter::new(writer, 0)),
            versions,
            version_set,
            kernal: kernel,
            options,
            manifest_file_number,
        });
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
