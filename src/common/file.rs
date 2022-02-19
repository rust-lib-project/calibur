use super::{Error, Result};
use std::path::PathBuf;

const ROCKS_DB_TFILE_EXT: &str = "sst";
const UNENCRYPTED_TEMP_FILE_NAME_SUFFIX: &str = "dbtmp.plain";

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum DBFileType {
    Current,
    DescriptorFile,
    LogFile,
}

pub fn make_current_file(path: &str) -> PathBuf {
    let p = format!("{}/CURRENT", path);
    PathBuf::from(p)
}

pub fn make_descriptor_file_name(path: &str, number: u64) -> PathBuf {
    let p = format!("{}/MANIFEST-{:06}", path, number);
    PathBuf::from(p)
}

pub fn make_file_name(path: &str, number: u64, suffix: &str) -> PathBuf {
    let p = format!("{}/{:06}.{}", path, number, suffix);
    PathBuf::from(p)
}

pub fn make_log_file(path: &str, number: u64) -> PathBuf {
    make_file_name(path, number, "log")
}

// TODO: support multi path
pub fn make_table_file_name(path: &str, number: u64) -> PathBuf {
    make_file_name(path, number, ROCKS_DB_TFILE_EXT)
}

pub fn make_temp_plain_file_name(path: &str, number: u64) -> PathBuf {
    make_file_name(path, number, ROCKS_DB_TFILE_EXT)
}

pub fn parse_file_name(fname: &str) -> Result<(DBFileType, u64)> {
    if fname == "CURRENT" {
        return Ok((DBFileType::Current, 0));
    } else if fname.starts_with("MANIFEST-") {
        let prefix = fname.trim_start_matches("MANIFEST-");
        let y = prefix.parse::<u64>().map_err(|e| {
            Error::InvalidFile(format!(
                "cant not parse file {} to manifest, Error: {:?}",
                fname, e
            ))
        })?;
        return Ok((DBFileType::DescriptorFile, y));
    } else if fname.ends_with(".log") {
        let prefix = fname.trim_end_matches(".log");
        let y = prefix.parse::<u64>().map_err(|e| {
            Error::InvalidFile(format!(
                "cant not parse file {} to manifest, Error: {:?}",
                fname, e
            ))
        })?;
        return Ok((DBFileType::LogFile, y));
    }
    Err(Error::InvalidFile(format!("cant parse file {}", fname)))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_file() {
        let p = make_descriptor_file_name("/home/rocksdb", 123);
        let m = PathBuf::from("/home/rocksdb/MANIFEST-000123");
        assert_eq!(p, m);
        let (tp, n) = parse_file_name(m.file_name().unwrap().to_str().unwrap()).unwrap();
        assert_eq!(tp, DBFileType::DescriptorFile);
        assert_eq!(n, 123);
        let p = make_log_file("/home/rocksdb", 456);
        let l = PathBuf::from("/home/rocksdb/000456.log");
        assert_eq!(p, l);
        let (tp, n) = parse_file_name(l.file_name().unwrap().to_str().unwrap()).unwrap();
        assert_eq!(tp, DBFileType::LogFile);
        assert_eq!(n, 456);
    }
}
