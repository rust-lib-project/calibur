// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::Arc;

use fail::fail_point;
use log::error;
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{close, ftruncate, lseek, Whence};
use nix::NixPath;

use crate::common::Result;

use super::format::LogFileHeader;

const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

/// A `LogFd` is a RAII file that provides basic I/O functionality.
///
/// This implementation is a thin wrapper around `RawFd`, and primarily targets
/// UNIX-based systems.
pub struct RawFile(RawFd);

fn from_nix_error(e: nix::Error, custom: &'static str) -> std::io::Error {
    let kind = std::io::Error::from(e).kind();
    std::io::Error::new(kind, custom)
}

impl RawFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let flags = OFlag::O_RDWR;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        Ok(RawFile(
            fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?,
        ))
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        fail_point!("log_fd::create::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        let fd = fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?;
        Ok(RawFile(fd))
    }

    pub fn close(&self) -> IoResult<()> {
        fail_point!("log_fd::close::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        close(self.0).map_err(|e| from_nix_error(e, "close"))
    }

    pub fn sync(&self) -> IoResult<()> {
        fail_point!("log_fd::sync::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        #[cfg(target_os = "linux")]
        {
            nix::unistd::fdatasync(self.0).map_err(|e| from_nix_error(e, "fdatasync"))
        }
        #[cfg(not(target_os = "linux"))]
        {
            nix::unistd::fsync(self.0).map_err(|e| from_nix_error(e, "fsync"))
        }
    }

    pub fn read(&self, mut offset: usize, buf: &mut [u8]) -> IoResult<usize> {
        let mut readed = 0;
        while readed < buf.len() {
            fail_point!("log_fd::read::err", |_| {
                Err(from_nix_error(nix::Error::EINVAL, "fp"))
            });
            let bytes = match pread(self.0, &mut buf[readed..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EAGAIN => continue,
                Err(e) => return Err(from_nix_error(e, "pread")),
            };
            // EOF
            if bytes == 0 {
                break;
            }
            readed += bytes;
            offset += bytes;
        }
        Ok(readed)
    }

    pub fn write(&self, mut offset: usize, content: &[u8]) -> IoResult<usize> {
        fail_point!("log_fd::write::zero", |_| { Ok(0) });
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EAGAIN => continue,
                Err(e) => return Err(from_nix_error(e, "pwrite")),
            };
            if bytes == 0 {
                break;
            }
            written += bytes;
            offset += bytes;
        }
        fail_point!("log_fd::write::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        Ok(written)
    }

    pub fn file_size(&self) -> IoResult<usize> {
        fail_point!("log_fd::file_size::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| from_nix_error(e, "lseek"))
    }

    pub fn truncate(&self, offset: usize) -> IoResult<()> {
        fail_point!("log_fd::truncate::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        ftruncate(self.0, offset as i64).map_err(|e| from_nix_error(e, "ftruncate"))
    }

    #[allow(unused_variables)]
    pub fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        fail_point!("log_fd::allocate::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        #[cfg(target_os = "linux")]
        {
            fcntl::fallocate(
                self.0,
                fcntl::FallocateFlags::empty(),
                offset as i64,
                size as i64,
            )
            .map_err(|e| from_nix_error(e, "fallocate"))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(())
        }
    }
}

impl Drop for RawFile {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("error while closing file: {}", e);
        }
    }
}

/// A `WritableFile` is a `RawFile` wrapper that implements `Seek`, `Write` and `Read`.
pub struct WritableFile {
    inner: Arc<RawFile>,
    offset: usize,
    capacity: usize,
}

impl WritableFile {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let fd = RawFile::open(path)?;
        Ok(Self::new(Arc::new(fd)))
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let fd = RawFile::create(path)?;
        Ok(Self::new(Arc::new(fd)))
    }

    pub fn new(fd: Arc<RawFile>) -> Self {
        let file_size = fd.file_size()?;
        Self {
            inner: fd,
            offset: 0,
            capacity: file_size,
        }
    }

    pub fn sync(&self) -> IoResult<()> {
        self.inner.sync()
    }
}

impl Write for WritableFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let new_written = self.offset + buf.len();
        if new_written > self.capacity {
            let alloc = std::cmp::max(new_written - self.capacity, FILE_ALLOCATE_SIZE);
            let mut real_alloc = FILE_ALLOCATE_SIZE;
            while real_alloc < alloc {
                real_alloc *= 2;
            }
            self.inner.allocate(self.capacity, real_alloc)?;
            self.capacity += real_alloc;
        }
        let len = self.inner.write(self.offset, buf)?;
        self.offset += len;
        Ok(len)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

/// Append-only writer for log file.
pub struct LogFileWriter {
    fd: Arc<RawFile>,
    writer: WritableFile,

    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl LogFileWriter {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        let fd = Arc::new(RawFile::open(path)?);
        let writer = WritableFile::new(fd.clone());
        let file_size = fd.file_size()?;
        let mut f = Self {
            fd,
            writer,
            written: file_size,
            capacity: file_size,
            last_sync: file_size,
        };
        if file_size < LogFileHeader::len() {
            f.write_header()?;
        } else {
            f.writer.seek(SeekFrom::Start(file_size as u64))?;
        }
        Ok(f)
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(SeekFrom::Start(0))?;
        self.written = 0;
        let mut buf = Vec::with_capacity(LogFileHeader::len());
        LogFileHeader::default().encode(&mut buf)?;
        self.write(&buf, 0)
    }

    pub fn close(&mut self) -> Result<()> {
        // Necessary to truncate extra zeros from fallocate().
        self.truncate()?;
        self.sync()
    }

    pub fn truncate(&mut self) -> Result<()> {
        if self.written < self.capacity {
            self.fd.truncate(self.written)?;
            self.capacity = self.written;
        }
        Ok(())
    }

    pub fn write(&mut self, buf: &[u8], target_size_hint: usize) -> Result<()> {
        let new_written = self.written + buf.len();
        if self.capacity < new_written {
            let alloc = std::cmp::max(
                new_written - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_size_hint.saturating_sub(self.capacity),
                ),
            );
            self.fd.allocate(self.capacity, alloc)?;
            self.capacity += alloc;
        }
        self.writer.write_all(buf)?;
        self.written = new_written;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.last_sync < self.written {
            self.fd.sync()?;
            self.last_sync = self.written;
        }
        Ok(())
    }

    #[inline]
    pub fn since_last_sync(&self) -> usize {
        self.written - self.last_sync
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.written
    }
}
