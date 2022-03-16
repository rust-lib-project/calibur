use crate::common::{Error, Result};
use crate::table::block_based::compression::{
    CompressionAlgorithm, CompressionInfo, UncompressionInfo,
};
use crate::util::{encode_var_uint32, get_var_uint32};
use libc::c_int;
use lz4::liblz4::{
    LZ4_compressBound, LZ4_compress_continue, LZ4_createStream, LZ4_createStreamDecode,
    LZ4_decompress_safe_continue, LZ4_freeStream, LZ4_freeStreamDecode,
};

#[derive(Default)]
pub struct LZ4CompressionAlgorithm {}

impl CompressionAlgorithm for LZ4CompressionAlgorithm {
    fn compress(
        &self,
        _info: &CompressionInfo,
        format_version: u32,
        data: &[u8],
    ) -> crate::Result<Vec<u8>> {
        if data.len() > u32::MAX as usize {
            return Err(Error::CompactionError(
                "can not compress block larger than 4GB".to_string(),
            ));
        }
        if format_version != 2 {
            unimplemented!();
        }
        unsafe {
            let mut tmp: [u8; 5] = [0u8; 5];
            let output_header_len = encode_var_uint32(&mut tmp, data.len() as u32);

            let compress_bound = LZ4_compressBound(data.len() as c_int);
            let mut output = vec![0u8; output_header_len + compress_bound as usize];
            output[..output_header_len].copy_from_slice(&tmp[..output_header_len]);
            let stream = LZ4_createStream();
            let outlen = LZ4_compress_continue(
                stream,
                data.as_ptr(),
                output.as_mut_ptr().add(output_header_len),
                data.len() as c_int,
            );
            LZ4_freeStream(stream);
            output.resize(outlen as usize + output_header_len, 0);
            Ok(output)
        }
    }

    fn name(&self) -> &'static str {
        "lz4"
    }

    fn uncompress(
        &self,
        _info: &UncompressionInfo,
        format_version: u32,
        origin_data: &[u8],
    ) -> Result<Vec<u8>> {
        if format_version != 2 {
            unimplemented!();
        }
        let mut offset = 0;
        let l = get_var_uint32(origin_data, &mut offset)
            .ok_or(Error::VarDecode("uncompress failed"))?;
        let compressed_size = origin_data.len() - offset as usize;
        println!("offset {}, length: {}, l {}", offset, compressed_size, l);
        let mut output = vec![0u8; l as usize];
        unsafe {
            let stream = LZ4_createStreamDecode();
            let decompress_size = LZ4_decompress_safe_continue(
                stream,
                origin_data.as_ptr().add(offset),
                output.as_mut_ptr(),
                compressed_size as i32,
                l as i32,
            );
            LZ4_freeStreamDecode(stream);
            if decompress_size < 0 {
                return Err(Error::VarDecode("decompress data failed"));
            }
            assert_eq!(decompress_size, l as i32);
            Ok(output)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_algorithm() {
        let lz4 = LZ4CompressionAlgorithm::default();
        let mut data = Vec::with_capacity(4096);
        for i in 0..100u64 {
            data.extend_from_slice(&i.to_le_bytes());
        }
        let output = lz4.compress(&CompressionInfo {}, 2, &data).unwrap();
        let origin = lz4.uncompress(&UncompressionInfo {}, 2, &output).unwrap();
        assert_eq!(data, origin);
    }
}
