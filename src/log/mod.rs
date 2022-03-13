mod reader;
mod writer;

pub const HEADER_SIZE: usize = 4 + 2 + 1;
pub const RECYCLABLE_HEADER_SIZE: usize = 4 + 2 + 1 + 4;

#[cfg(test)]
pub const BLOCK_SIZE: usize = 4096;
#[cfg(not(test))]
pub const BLOCK_SIZE: usize = 32768;
pub const LOG_PADDING: &[u8] = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum RecordType {
    // Zero is reserved for preallocated files
    ZeroType = 0,
    FullType = 1,

    // For fragments
    FirstType = 2,
    MiddleType = 3,
    LastType = 4,
    // For recycled log files
    RecyclableFullType = 5,
    // RecyclableFirstType = 6,
    // RecyclableMiddleType = 7,
    RecyclableLastType = 8,
    Unknown = 127,
}

impl From<u8> for RecordType {
    fn from(x: u8) -> Self {
        if x > 8 {
            RecordType::Unknown
        } else {
            unsafe { std::mem::transmute(x) }
        }
    }
}

const MAX_RECORD_TYPE: u8 = RecordType::RecyclableLastType as u8;

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum RecordError {
    Eof = 9,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    BadRecord = 10,
    // Returned when we fail to read a valid header.
    BadHeader = 11,
    // Returned when we read an old record from a previous user of the log.
    OldRecord = 12,
    // Returned when we get a bad record length
    BadRecordLen = 13,
    // Returned when we get a bad record checksum
    BadRecordChecksum = 14,
    Unknown = 127,
}

impl From<u8> for RecordError {
    fn from(x: u8) -> Self {
        if !(9..=14).contains(&x) {
            RecordError::Unknown
        } else {
            unsafe { std::mem::transmute(x) }
        }
    }
}

pub use reader::LogReader;
pub use writer::LogWriter;
