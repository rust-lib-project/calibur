use crate::common::Result;

#[derive(Default)]
pub struct CompressionInfo {}

pub struct UncompressionInfo {}

pub trait CompressionAlgorithm {
    fn compress(&self, info: &CompressionInfo, format_version: u32, data: &[u8])
        -> Result<Vec<u8>>;
    fn name(&self) -> &'static str;
    fn uncompress(
        &self,
        info: &UncompressionInfo,
        format_version: u32,
        origin_data: &[u8],
    ) -> Result<Vec<u8>>;
}
