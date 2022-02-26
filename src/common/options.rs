#[derive(Eq, PartialEq, Clone)]
pub enum CompressionType {
    NoCompression = 0x0,
    SnappyCompression = 0x1,
    ZlibCompression = 0x2,
    BZip2Compression = 0x3,
    LZ4Compression = 0x4,
    LZ4HCCompression = 0x5,
    XpressCompression = 0x6,
    ZSTD = 0x7,

    // Only use ZSTDNotFinalCompression if you have to use ZSTD lib older than
    // 0.8.0 or consider a possibility of downgrading the service or copying
    // the database files to another service running with an older version of
    // RocksDB that doesn't have ZSTD. Otherwise, you should use ZSTD. We will
    // eventually remove the option from the public API.
    ZSTDNotFinalCompression = 0x40,

    // DisableCompressionOption is used to disable some compression options.
    DisableCompressionOption = 0xff,
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    pub snapshot: Option<u64>,
    pub fill_cache: bool,
    pub total_order_seek: bool,
    pub prefix_same_as_start: bool,
    pub skip_filter: bool,
}
