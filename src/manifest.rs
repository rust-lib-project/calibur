use crate::common::{
    make_current_file, make_descriptor_file_name, make_table_file_name, parse_file_name,
    DBFileType, Error, FileSystem, Result,
};
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};

use crate::compaction::CompactionEngine;
use crate::log::{LogReader, LogWriter};
use crate::options::{ColumnFamilyDescriptor, ImmutableDBOptions};
use crate::table::TableReaderOptions;
use crate::version::{KernelNumberContext, TableFile, Version, VersionEdit, VersionSet};
use crate::ColumnFamilyOptions;
use futures::SinkExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const MAX_BATCH_SIZE: usize = 128;

pub struct Manifest {
    log: Option<Box<LogWriter>>,
    versions: HashMap<u32, Arc<Version>>,
    cf_options: HashMap<u32, Arc<ColumnFamilyOptions>>,
    version_set: Arc<Mutex<VersionSet>>,
    kernel: Arc<KernelNumberContext>,
    options: Arc<ImmutableDBOptions>,
    manifest_file_number: u64,
    // only used for delete
    files_by_id: HashMap<u64, Arc<TableFile>>,
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
        let kernal = version_set.get_kernel();

        let cf_versions = version_set.get_column_family_versions();
        let mut versions = HashMap::default();
        for v in cf_versions {
            versions.insert(v.get_cf_id(), v);
        }

        let mut cf_options = HashMap::default();
        let opts = version_set.get_column_family_options();
        for (cf, opt) in opts {
            cf_options.insert(cf, opt);
        }

        Ok(Self {
            log: None,
            version_set: Arc::new(Mutex::new(version_set)),
            manifest_file_number,
            versions,
            kernel: kernal,
            options: db_options.clone(),
            cf_options,
            files_by_id: Default::default(),
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
        let mut edits_by_cf = HashMap::<u32, Vec<VersionEdit>>::new();
        for e in edits {
            match edits_by_cf.get_mut(&e.column_family) {
                Some(v) => {
                    v.push(e);
                }
                None => {
                    let cf = e.column_family;
                    let v = vec![e];
                    edits_by_cf.insert(cf, v);
                }
            }
        }
        for (cf, mut edits) in edits_by_cf {
            if edits.len() == 1 && edits[0].is_column_family_add {
                let mut version_set = self.version_set.lock().unwrap();
                let version = version_set.create_column_family(edits.pop().unwrap())?;
                self.versions.insert(cf, version);
                continue;
            }
            // TODO: get options from version_set
            let opts = self.cf_options.get(&cf).unwrap();
            let version = self.versions.get(&cf).unwrap();
            let mut mems = vec![];
            let mut to_add = vec![];
            let mut to_delete = vec![];
            // Do not apply version edits with holding a mutex.
            let mut log_number = 0;
            for mut e in edits {
                if e.has_log_number {
                    log_number = std::cmp::max(log_number, e.log_number);
                }
                mems.append(&mut e.mems_deleted);
                for m in e.deleted_files {
                    if let Some(f) = self.files_by_id.remove(&m.fd.get_number()) {
                        to_delete.push(f);
                    }
                }
                for m in e.add_files {
                    let fname = make_table_file_name(&self.options.db_path, m.id());
                    let file = self.options.fs.open_random_access_file(fname.clone())?;
                    let mut read_opts = TableReaderOptions::default();
                    read_opts.file_size = m.fd.file_size as usize;
                    read_opts.level = m.level;
                    read_opts.largest_seqno = m.fd.largest_seqno;
                    read_opts.internal_comparator = opts.comparator.clone();
                    read_opts.prefix_extractor = opts.prefix_extractor.clone();
                    let table_reader = opts.factory.open_reader(&read_opts, file).await?;
                    let table = Arc::new(TableFile::new(
                        m,
                        table_reader,
                        self.options.fs.clone(),
                        fname,
                    ));
                    to_add.push(table);
                }
            }
            let new_version = version.apply(to_add, to_delete, log_number);
            let mut version_set = self.version_set.lock().unwrap();
            let version = version_set.install_version(cf, mems, new_version)?;
            self.versions.insert(cf, version);
        }
        self.log.as_mut().unwrap().fsync().await?;
        Ok(())
    }

    async fn write_snapshot(&mut self, writer: &mut LogWriter) -> Result<()> {
        let versions = {
            let version_set = self.version_set.lock().unwrap();
            version_set.get_column_family_versions()
        };
        for version in versions {
            let mut record = Vec::new();
            {
                let mut edit = VersionEdit::default();
                edit.column_family_name = version.get_cf_name().to_string();
                edit.column_family = version.get_cf_id() as u32;
                edit.is_column_family_add = true;
                edit.comparator_name = version.get_comparator_name().to_string();
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

    pub fn batch(&mut self, mut task: ManifestTask) -> bool {
        let mut need_apply = self.edits.len() > MAX_BATCH_SIZE;
        if task.edits.len() == 1
            && (task.edits[0].is_column_family_add || task.edits[0].is_column_family_drop)
        {
            need_apply = true;
        }
        self.edits.append(&mut task.edits);
        self.cbs.push(task.cb);
        need_apply
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
    pub edits: Vec<VersionEdit>,
    pub cb: OnceSender<Result<()>>,
}

#[derive(Clone)]
pub struct ManifestScheduler {
    sender: UnboundedSender<ManifestTask>,
}

impl ManifestScheduler {
    pub fn new(sender: UnboundedSender<ManifestTask>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl CompactionEngine for ManifestScheduler {
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
