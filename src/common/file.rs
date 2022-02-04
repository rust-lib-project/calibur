use std::path::PathBuf;

const ROCKS_DB_TFILE_EXT: &str = "sst";

// TODO: support multi path
pub fn make_table_file_name(path: &str, number: u64) -> PathBuf {
    make_file_name(path, number, ROCKS_DB_TFILE_EXT)
}

pub fn make_file_name(path: &str, number: u64, suffix: &str) -> PathBuf {
    let p = format!("{}/{:06}.{}", path, number, suffix);
    PathBuf::from(p)
}
