use crate::common::{Error, InternalKeyComparator, Result};
use crate::memtable::Memtable;
use crate::table::{InternalIterator, MergingIterator};
use crate::version::VersionEdit;
use futures::channel::mpsc::Sender;
use futures::channel::oneshot::{
    channel as once_channel, Receiver as OnceReceiver, Sender as OnceSender,
};

use crate::compaction::CompactionEngine;
use crate::version::manifest::Manifest;
use futures::SinkExt;
use std::sync::{Arc, Mutex};

pub struct ManifestWriter {
    manifest: Box<Manifest>,
}

pub struct ManifestTask {
    edits: Vec<VersionEdit>,
    cb: OnceSender<()>,
}

#[derive(Clone)]
pub struct ManifestEngine {
    sender: Sender<ManifestTask>,
    comparator: InternalKeyComparator,
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
        rx.await.map_err(|e| {
            Error::Cancel(format!(
                "The manifest thread may cancel this apply, {:?}",
                e
            ))
        })
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
