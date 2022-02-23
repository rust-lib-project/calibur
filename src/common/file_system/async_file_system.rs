use std::fs::{read_dir, rename};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use std::{ptr::null, slice};

use super::{Error, Result};
use crate::common::file_system::posix_file_system::RawFile;
use crate::common::{
    RandomAccessFile, RandomAccessFileReader, SequentialFile, SequentialFileReader, WritableFile,
    WritableFileWriter,
};

use crate::common::file_system::IOOption;
use crate::FileSystem;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use futures::channel::oneshot::{channel as once_channel, Sender as OnceSender};

const STOP_ERROR: &str = "IO failed because read pool has stopped";
const CANCEL_ERROR: &str = "IO failed because task was canceled";
struct ReadTask {
    ptr: *mut u8,
    data_size: usize,
    fd: Arc<RawFile>,
    offset: usize,
    cb: OnceSender<Result<usize>>,
}

unsafe impl Send for ReadTask {}
unsafe impl Sync for ReadTask {}
impl ReadTask {
    // Do not call it self again
    fn run(self) {
        unsafe {
            let buf = slice::from_raw_parts_mut(self.ptr, self.data_size);
            let ret = self
                .fd
                .read(self.offset, buf)
                .map_err(|e| Error::Io(Box::new(e)));
            let _ = self.cb.send(ret);
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum IOOperation {
    Fsync,
    Write,
    Truncate,
}

struct WriteTask {
    fd: Arc<RawFile>,
    ptr: *const u8,
    data_size: usize,
    offset: usize,
    op: IOOperation,
    cb: OnceSender<Result<usize>>,
}

unsafe impl Send for WriteTask {}
unsafe impl Sync for WriteTask {}

impl WriteTask {
    // Do not call it self again
    pub fn run(self) {
        unsafe {
            let mut ret = Ok(0);
            if self.op == IOOperation::Truncate {
                ret = self
                    .fd
                    .truncate(self.offset)
                    .map_err(|e| Error::Io(Box::new(e)))
                    .map(|_| 0);
            } else if self.data_size > 0 {
                let buf = slice::from_raw_parts(self.ptr, self.data_size);
                ret = self
                    .fd
                    .write(self.offset, buf)
                    .map_err(|e| Error::Io(Box::new(e)));
            }
            if ret.is_ok() && self.op == IOOperation::Fsync {
                if let Err(e) = self.fd.sync() {
                    ret = Err(Error::Io(Box::new(e)));
                }
            }
            let _ = self.cb.send(ret);
        }
    }
}

pub struct AsyncContext {
    read_queue: ArrayQueue<ReadTask>,
    write_queue: ArrayQueue<WriteTask>,
    high_write_queue: ArrayQueue<WriteTask>,
    worker_thread_count: AtomicUsize,
    total_thread_count: usize,
    thread_state: Mutex<Vec<bool>>,
    thread_conv: Condvar,
    closed: AtomicBool,
}

impl AsyncContext {
    fn new(total_thread_count: usize) -> Self {
        Self {
            read_queue: ArrayQueue::new(1024),
            write_queue: ArrayQueue::new(1024),
            high_write_queue: ArrayQueue::new(16),
            worker_thread_count: AtomicUsize::new(total_thread_count),
            total_thread_count,
            thread_state: Mutex::new(vec![false; total_thread_count]),
            thread_conv: Condvar::default(),
            closed: AtomicBool::new(false),
        }
    }

    fn wait(&self, id: usize) {
        let mut state = self.thread_state.lock().unwrap();
        if !self.high_write_queue.is_empty()
            || !self.write_queue.is_empty()
            || !self.read_queue.is_empty()
        {
            return;
        }
        (*state)[id] = true;
        self.worker_thread_count.fetch_sub(1, Ordering::SeqCst);
        while (*state)[id] {
            state = self.thread_conv.wait(state).unwrap();
        }
        self.worker_thread_count.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_up_one(&self) {
        if self.worker_thread_count.load(Ordering::Acquire) >= self.total_thread_count {
            return;
        }
        let mut state = self.thread_state.lock().unwrap();
        for t in state.iter_mut() {
            if *t {
                *t = false;
                break;
            }
        }
        self.thread_conv.notify_all();
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            while let Some(t) = self.read_queue.pop() {
                let _ = t.cb.send(Err(Error::Cancel(STOP_ERROR)));
            }
            while let Some(t) = self.write_queue.pop() {
                let _ = t.cb.send(Err(Error::Cancel(STOP_ERROR)));
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn busy_worker_count(&self) -> usize {
        self.worker_thread_count.load(Ordering::Acquire)
    }
}

/// A `WritableFile` is a `RawFile` wrapper that implements `Write`.
pub struct AsyncWritableFile {
    inner: Arc<RawFile>,
    ctx: Arc<AsyncContext>,
    offset: usize,
    high_priority: bool,
}

impl AsyncWritableFile {
    fn open(path: &PathBuf, ctx: Arc<AsyncContext>, high_priority: bool) -> Result<Self> {
        let fd = RawFile::create(path).map_err(|e| Error::Io(Box::new(e)))?;
        Ok(Self {
            inner: Arc::new(fd),
            offset: 0,
            ctx,
            high_priority,
        })
    }

    fn run_async_task(&self, mut task: WriteTask) -> Result<()> {
        if self.ctx.closed.load(Ordering::Acquire) {
            return Err(Error::Cancel(STOP_ERROR));
        }
        if self.high_priority {
            let mut ret = self.ctx.high_write_queue.push(task);
            self.ctx.wake_up_one();
            while let Err(t) = ret {
                if self.ctx.closed.load(Ordering::Acquire) {
                    return Err(Error::Cancel(STOP_ERROR));
                }
                ret = self.ctx.high_write_queue.push(t);
                self.ctx.wake_up_one();
            }
        } else {
            while let Err(t) = self.ctx.write_queue.push(task) {
                if self.ctx.closed.load(Ordering::Acquire) {
                    return Err(Error::Cancel(STOP_ERROR));
                }
                if self.ctx.busy_worker_count() <= 1 {
                    self.ctx.wake_up_one();
                }
                std::thread::sleep(Duration::from_millis(1));
                task = t;
            }
            if self.ctx.busy_worker_count() <= 1 {
                self.ctx.wake_up_one();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WritableFile for AsyncWritableFile {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        let (cb, rc) = once_channel();
        let task = WriteTask {
            fd: self.inner.clone(),
            ptr: data.as_ptr(),
            data_size: data.len(),
            offset: self.offset,
            op: IOOperation::Write,
            cb,
        };
        self.run_async_task(task)?;
        let ret = rc.await.map_err(|_| Error::Cancel(CANCEL_ERROR))?;
        let written = ret?;
        self.offset += written;
        Ok(())
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        let (cb, rc) = once_channel();
        let task = WriteTask {
            fd: self.inner.clone(),
            ptr: null(),
            data_size: 0,
            offset: offset as usize,
            op: IOOperation::Truncate,
            cb,
        };
        self.run_async_task(task)?;
        let ret = rc.await.map_err(|_| Error::Cancel(CANCEL_ERROR))?;
        ret?;
        Ok(())
    }

    fn allocate(&mut self, _offset: u64, _len: u64) -> Result<()> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        let (cb, rc) = once_channel();
        let task = WriteTask {
            fd: self.inner.clone(),
            ptr: null(),
            data_size: 0,
            offset: self.offset,
            op: IOOperation::Fsync,
            cb,
        };
        self.run_async_task(task)?;
        let ret = rc.await.map_err(|_| Error::Cancel(CANCEL_ERROR))?;
        ret?;
        Ok(())
    }

    async fn fsync(&mut self) -> Result<()> {
        self.sync().await
    }
}
/// A `WritableFile` is a `RawFile` wrapper that implements `Write`.
pub struct AsyncRandomAccessFile {
    fd: Arc<RawFile>,
    ctx: Arc<AsyncContext>,
    file_size: usize,
}

impl AsyncRandomAccessFile {
    fn open(path: &PathBuf, ctx: Arc<AsyncContext>) -> Result<Self> {
        let fd = RawFile::open_for_read(path, true).map_err(|e| Error::Io(Box::new(e)))?;
        let file_size = fd.file_size().map_err(|e| Error::Io(Box::new(e)))?;
        Ok(Self {
            fd: Arc::new(fd),
            file_size,
            ctx,
        })
    }

    fn run_async_task(&self, mut task: ReadTask) -> Result<()> {
        if self.ctx.closed.load(Ordering::Acquire) {
            return Err(Error::Cancel(STOP_ERROR));
        }
        while let Err(t) = self.ctx.read_queue.push(task) {
            if self.ctx.closed.load(Ordering::Acquire) {
                return Err(Error::Cancel(STOP_ERROR));
            }
            if self.ctx.busy_worker_count() <= 1 {
                self.ctx.wake_up_one();
            }
            std::thread::sleep(Duration::from_millis(1));
            task = t;
        }
        if self.ctx.busy_worker_count() <= 1 {
            self.ctx.wake_up_one();
        }
        Ok(())
    }
}

#[async_trait]
impl RandomAccessFile for AsyncRandomAccessFile {
    async fn read(&self, offset: usize, data: &mut [u8]) -> Result<usize> {
        let (cb, rc) = once_channel();
        let task = ReadTask {
            fd: self.fd.clone(),
            ptr: data.as_mut_ptr(),
            data_size: data.len(),
            offset: offset as usize,
            cb,
        };
        self.run_async_task(task)?;
        let ret = rc.await.map_err(|_| Error::Cancel(CANCEL_ERROR))?;
        let n = ret?;
        Ok(n)
    }

    async fn read_exact(&self, offset: usize, n: usize, data: &mut [u8]) -> Result<usize> {
        let (cb, rc) = once_channel();
        let task = ReadTask {
            fd: self.fd.clone(),
            ptr: data.as_mut_ptr(),
            data_size: std::cmp::min(n, data.len()),
            offset: offset as usize,
            cb,
        };
        self.run_async_task(task)?;
        let ret = rc.await.map_err(|_| Error::Cancel(CANCEL_ERROR))?;
        let n = ret?;
        Ok(n)
    }

    fn file_size(&self) -> usize {
        self.file_size
    }
}

pub struct AsyncSequentialFile {
    inner: AsyncRandomAccessFile,
    offset: usize,
}

impl AsyncSequentialFile {
    fn open(path: &PathBuf, ctx: Arc<AsyncContext>) -> Result<Self> {
        let inner = AsyncRandomAccessFile::open(path, ctx)?;
        Ok(Self { inner, offset: 0 })
    }
}

#[async_trait]
impl SequentialFile for AsyncSequentialFile {
    async fn read_sequencial(&mut self, data: &mut [u8]) -> Result<usize> {
        let (cb, rc) = once_channel();
        let task = ReadTask {
            fd: self.inner.fd.clone(),
            ptr: data.as_mut_ptr(),
            data_size: data.len(),
            offset: self.offset,
            cb,
        };
        self.inner.run_async_task(task)?;
        let ret = rc.await.map_err(|_| Error::Cancel(CANCEL_ERROR))?;
        let n = ret?;
        self.offset += n;
        Ok(n)
    }

    fn get_file_size(&self) -> usize {
        self.inner.file_size()
    }
}

pub struct AsyncFileSystem {
    ctx: Arc<AsyncContext>,
    pool_handles: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl AsyncFileSystem {
    pub fn new(pool_size: usize) -> Self {
        let ctx = Arc::new(AsyncContext::new(pool_size));
        let mut pool_handles = vec![];
        for i in 0..pool_size {
            let wctx = ctx.clone();
            let h = thread::spawn(move || {
                if i == 0 {
                    run_high_io_task(wctx);
                } else {
                    run_io_task(wctx, i);
                }
            });
            pool_handles.push(h);
        }
        AsyncFileSystem {
            ctx,
            pool_handles: Mutex::new(pool_handles),
        }
    }

    fn open_writable_file(&self, path: PathBuf, opts: &IOOption) -> Result<WritableFileWriter> {
        let file_name = path
            .file_name()
            .ok_or(Error::InvalidFile(format!("path has no file name")))?
            .to_str()
            .ok_or(Error::InvalidFile(format!(
                "filename is not encode by utf8"
            )))?;
        let f = AsyncWritableFile::open(&path, self.ctx.clone(), opts.high_priority)?;
        Ok(WritableFileWriter::new(
            Box::new(f),
            file_name.to_string(),
            opts.buffer_size,
        ))
    }

    pub fn stop(&self) {
        self.ctx.close();
        let mut handles = self.pool_handles.lock().unwrap();
        for h in handles.drain(..) {
            h.join().unwrap();
        }
    }
}

impl FileSystem for AsyncFileSystem {
    fn open_writable_file_writer(&self, path: PathBuf) -> Result<Box<WritableFileWriter>> {
        let f = self.open_writable_file(path, &IOOption::default())?;
        Ok(Box::new(f))
    }

    fn open_writable_file_writer_opt(
        &self,
        path: PathBuf,
        opts: &IOOption,
    ) -> Result<Box<WritableFileWriter>> {
        let f = self.open_writable_file(path, opts)?;
        Ok(Box::new(f))
    }

    fn open_random_access_file(&self, p: PathBuf) -> Result<Box<RandomAccessFileReader>> {
        let f = AsyncRandomAccessFile::open(&p, self.ctx.clone())?;
        let filename = p
            .file_name()
            .ok_or(Error::InvalidFile(format!("path has no file name")))?
            .to_str()
            .ok_or(Error::InvalidFile(format!(
                "filename is not encode by utf8"
            )))?;
        let reader = RandomAccessFileReader::new(Box::new(f), filename.to_string());
        Ok(Box::new(reader))
    }

    fn open_sequencial_file(&self, path: PathBuf) -> Result<Box<SequentialFileReader>> {
        let f = AsyncSequentialFile::open(&path, self.ctx.clone())?;
        let reader = SequentialFileReader::new(
            Box::new(f),
            path.file_name().unwrap().to_str().unwrap().to_string(),
        );
        Ok(Box::new(reader))
    }

    fn remove(&self, path: PathBuf) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| Error::Io(Box::new(e)))
    }

    fn rename(&self, origin: PathBuf, target: PathBuf) -> Result<()> {
        rename(origin, target).map_err(|e| Error::Io(Box::new(e)))
    }

    fn list_files(&self, path: PathBuf) -> Result<Vec<PathBuf>> {
        let mut files = vec![];
        for f in read_dir(path).map_err(|e| Error::Io(Box::new(e)))? {
            files.push(f?.path());
        }
        Ok(files)
    }

    fn file_exist(&self, path: &PathBuf) -> Result<bool> {
        Ok(path.exists())
    }
}

pub fn run_high_io_task(ctx: Arc<AsyncContext>) {
    while !ctx.is_closed() {
        while let Some(t) = ctx.high_write_queue.pop() {
            t.run();
        }
        if let Some(t) = ctx.write_queue.pop() {
            t.run();
        } else if let Some(t) = ctx.read_queue.pop() {
            t.run();
        }
        if let Some(t) = spin_for_task(&ctx.high_write_queue) {
            t.run();
            continue;
        }
        ctx.wait(0);
    }
}

pub fn run_io_task(ctx: Arc<AsyncContext>, idx: usize) {
    while !ctx.is_closed() {
        let mut processed_task_count = 0;
        while let Some(t) = ctx.read_queue.pop() {
            processed_task_count += 1;
            t.run();
            if processed_task_count > 10 {
                ctx.wake_up_one();
                break;
            }
        }
        processed_task_count = 0;
        while let Some(t) = ctx.write_queue.pop() {
            processed_task_count += 1;
            t.run();
            if processed_task_count > 10 {
                ctx.wake_up_one();
                break;
            }
        }
        if let Some(t) = ctx.write_queue.pop() {
            t.run();
            continue;
        }
        ctx.wait(idx);
    }
}

pub fn spin_for_task<T>(que: &ArrayQueue<T>) -> Option<T> {
    for _ in 0..100 {
        if let Some(t) = que.pop() {
            return Some(t);
        }
        thread::yield_now();
    }
    None
}
