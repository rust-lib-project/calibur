use crate::common::{
    make_current_file, make_descriptor_file_name, make_table_file_name, make_temp_plain_file_name,
    parse_file_name, DBFileType, Error, FileSystem, Result,
};
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};

use crate::compaction::CompactionEngine;
use crate::log::{LogReader, LogWriter};
use crate::options::{ColumnFamilyDescriptor, ImmutableDBOptions};
use crate::table::TableReaderOptions;
use crate::util::BtreeComparable;
use crate::version::{
    FileMetaData, KernelNumberContext, TableFile, Version, VersionEdit, VersionSet,
};
use crate::{ColumnFamilyOptions, KeyComparator};
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
    pub async fn create(
        cf_descriptor: &[ColumnFamilyDescriptor],
        db_options: &Arc<ImmutableDBOptions>,
    ) -> Result<Self> {
        let mut new_db = VersionEdit::default();
        new_db.set_log_number(0);
        new_db.set_next_file(2);
        new_db.set_last_sequence(0);
        let descrip_file_name = make_descriptor_file_name(&db_options.db_path, 1);
        let writer = db_options.fs.open_writable_file_writer(descrip_file_name)?;
        let mut writer = LogWriter::new(writer, 0);
        let mut buf = vec![];
        new_db.encode_to(&mut buf);
        writer.add_record(&buf).await?;
        writer.fsync().await?;
        store_current_file(&db_options.fs, 1, &db_options.db_path).await?;
        Self::recover(cf_descriptor, db_options).await
    }

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
        let (files_by_id, version_set) =
            Self::recover_version(cfs, Box::new(log_reader), db_options).await?;
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
            files_by_id,
        })
    }

    pub async fn recover_version(
        cf_descriptor: &[ColumnFamilyDescriptor],
        mut reader: Box<LogReader>,
        options: &ImmutableDBOptions,
    ) -> Result<(HashMap<u64, Arc<TableFile>>, VersionSet)> {
        // let cf_options = cfs.iter().map(|d|d.options.clone()).collect();
        let mut record = vec![];
        let mut edits: HashMap<u32, Vec<VersionEdit>> = HashMap::default();
        let kernel = Arc::new(KernelNumberContext::default());
        let mut cf_options: HashMap<String, ColumnFamilyOptions> = HashMap::default();
        for cf in cf_descriptor.iter() {
            cf_options.insert(cf.name.clone(), cf.options.clone());
        }
        if cf_options.get("default").is_none() {
            cf_options.insert("default".to_string(), ColumnFamilyOptions::default());
        }
        let mut cf_names: HashMap<u32, String> = HashMap::default();
        cf_names.insert(0, "default".to_string());
        edits.insert(0, vec![]);
        while reader.read_record(&mut record).await? {
            let mut edit = VersionEdit::default();
            edit.decode_from(&record)?;
            if edit.is_column_family_add {
                edits.insert(edit.column_family, vec![]);
                cf_names.insert(edit.column_family, edit.column_family_name);
            } else if edit.is_column_family_drop {
                edits.remove(&edit.column_family);
                cf_names.remove(&edit.column_family);
            } else {
                if let Some(data) = edits.get_mut(&edit.column_family) {
                    data.push(edit);
                }
            }
        }
        let mut versions = HashMap::default();
        let mut has_prev_log_number = false;
        let mut prev_log_number = 0;
        let mut has_next_file_number = false;
        let mut next_file_number = 0;
        let mut has_last_sequence = false;
        let mut last_sequence = 0;
        let mut has_max_column_family = false;
        let mut max_column_family = 0;
        let mut files = HashMap::default();
        for (cf_id, edit) in edits {
            let cf_name = cf_names.get(&cf_id).unwrap();
            let cf_opts = cf_options
                .remove(cf_name)
                .unwrap_or(ColumnFamilyOptions::default());

            let mut files_by_id: HashMap<u64, FileMetaData> = HashMap::new();
            let mut max_log_number = 0;
            for e in edit {
                if e.has_prev_log_number {
                    has_prev_log_number = true;
                    prev_log_number = e.prev_log_number;
                }
                if e.has_next_file_number {
                    has_next_file_number = true;
                    next_file_number = e.next_file_number;
                }
                if e.has_last_sequence {
                    has_last_sequence = true;
                    last_sequence = e.last_sequence;
                }
                if e.has_max_column_family {
                    has_max_column_family = true;
                    max_column_family = e.max_column_family;
                }
                max_log_number = std::cmp::max(max_log_number, e.log_number);
                for m in e.deleted_files {
                    files_by_id.remove(&m.id());
                }
                for m in e.add_files {
                    files_by_id.insert(m.id(), m);
                }
            }
            let mut tables = vec![];
            for (_, m) in files_by_id {
                let fname = make_table_file_name(&options.db_path, m.id());
                let file = options.fs.open_random_access_file(fname.clone())?;
                let mut read_opts = TableReaderOptions::default();
                read_opts.file_size = m.fd.file_size as usize;
                read_opts.level = m.level;
                read_opts.largest_seqno = m.fd.largest_seqno;
                read_opts.internal_comparator = cf_opts.comparator.clone();
                read_opts.prefix_extractor = cf_opts.prefix_extractor.clone();
                let table_reader = cf_opts.factory.open_reader(&read_opts, file).await?;
                let table = Arc::new(TableFile::new(m, table_reader, options.fs.clone(), fname));
                files.insert(table.meta.id(), table.clone());
                tables.push(table);
            }
            let version = Version::new(
                cf_id,
                cf_name.clone(),
                cf_opts.comparator.name().to_string(),
                tables,
                max_log_number,
                cf_opts.max_level,
            );
            versions.insert(cf_id, Arc::new(version));
        }
        if has_prev_log_number {
            kernel.mark_file_number_used(prev_log_number);
        }
        if has_next_file_number {
            kernel.mark_file_number_used(next_file_number);
        }
        if has_last_sequence {
            kernel.set_last_sequence(last_sequence);
        }
        if has_max_column_family {
            kernel.set_max_column_family(max_column_family);
        }
        let vs = VersionSet::new(cf_descriptor, kernel, options.fs.clone(), versions);
        Ok((files, vs))
    }

    pub fn get_version_set(&self) -> Arc<Mutex<VersionSet>> {
        self.version_set.clone()
    }

    pub async fn process_manifest_writes(&mut self, mut edits: Vec<VersionEdit>) -> Result<()> {
        if edits.is_empty() {
            return Ok(());
        }
        let mut new_descripter = false;
        if self.log.as_ref().map_or(true, |f| {
            f.get_file_size() > self.options.max_manifest_file_size
        }) {
            // TODO: Switch manifest log writer
            let file_number = self.kernel.new_file_number();
            let descrip_file_name = make_descriptor_file_name(&self.options.db_path, file_number);
            let writer = self
                .options
                .fs
                .open_writable_file_writer(descrip_file_name)?;
            let mut writer = LogWriter::new(writer, 0);
            self.write_snapshot(&mut writer).await?;
            self.log = Some(Box::new(writer));
            self.manifest_file_number = file_number;
            if let Some(edit) = edits.first_mut() {
                edit.set_max_column_family(self.kernel.get_max_column_family());
            }
            new_descripter = true;
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
            // TODO: get options from version_set
            if edits.len() == 1 && edits[0].is_column_family_add {
                let mut version_set = self.version_set.lock().unwrap();
                let version = version_set.create_column_family(edits.pop().unwrap())?;
                self.versions.insert(cf, version);
                continue;
            }
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
                    self.files_by_id.insert(table.id(), table.clone());
                    to_add.push(table);
                }
            }
            let new_version = version.apply(to_add, to_delete, log_number);
            let mut version_set = self.version_set.lock().unwrap();
            let version = version_set.install_version(cf, mems, new_version)?;
            self.versions.insert(cf, version);
        }
        self.log.as_mut().unwrap().fsync().await?;
        if new_descripter {
            store_current_file(
                &self.options.fs,
                self.manifest_file_number,
                &self.options.db_path,
            )
            .await?;
        }
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
                edit.column_family = version.get_cf_id() as u32;
                edit.add_column_family(version.get_cf_name().to_string());
                edit.set_comparator_name(version.get_comparator_name());
                edit.set_log_number(version.get_log_number());
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
            .map_err(|_| Error::Cancel("the manifest thread may close"))?;
        let x = rx
            .await
            .map_err(|_| Error::Cancel("The manifest thread may cancel this apply"))?;
        x
    }
}

pub async fn store_current_file(
    fs: &Arc<dyn FileSystem>,
    descriptor_number: u64,
    dbpath: &str,
) -> Result<()> {
    let fname = make_descriptor_file_name(dbpath, descriptor_number);
    let contents = fname.to_str().unwrap();
    let prefix = dbpath.to_string() + "/";
    let mut ret = contents.trim_start_matches(&prefix).to_string();
    ret.push('\n');
    let tmp = make_temp_plain_file_name(dbpath, descriptor_number);
    let mut writer = fs.open_writable_file_writer(tmp.clone())?;
    let data = ret.into_bytes();
    writer.append(&data).await?;
    writer.sync().await?;
    fs.rename(tmp, make_current_file(dbpath))
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
