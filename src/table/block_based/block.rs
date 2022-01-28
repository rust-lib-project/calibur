use crate::common::KeyComparator;
use crate::table::block_based::data_block_hash_index_builder::DataBlockHashIndex;
use crate::table::block_based::options::DataBlockIndexType;
use crate::table::format::MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX;
use crate::util::decode_fixed_uint32;
use std::sync::Arc;

const DATA_BLOCK_INDEX_TYPE_BIT_SHIFT: u32 = 31;

const MAX_NUM_RESTARTS: u32 = (1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT) - 1u32;

// 0x7FFFFFFF
const NUM_RESTARTS_MASK: u32 = (1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT) - 1u32;

pub struct Block {
    pub data: Vec<u8>,
    pub restart_offset: u32,
    pub num_restarts: u32,
    pub global_seqno: u64,
    data_block_hash_index: DataBlockHashIndex,
}

impl Block {
    pub fn new(data: Vec<u8>, global_seqno: u64) -> Self {
        if data.len() < std::mem::size_of::<u32>() {
            return Block {
                data: vec![],
                restart_offset: 0,
                num_restarts: 0,
                global_seqno,
                data_block_hash_index: DataBlockHashIndex::default(),
            };
        } else {
            let num_restarts = calculate_num_restarts(&data);
            match calculate_index_type(&data) {
                DataBlockIndexType::DataBlockBinarySearch => {
                    let restart_offset =
                        data.len() as u32 - (1 + num_restarts_) * std::mem::size_of::<u32>();
                    if restart_offset + std::mem::size_of::<u32>() > data.len() {
                        Block {
                            data: vec![],
                            data_block_hash_index: DataBlockHashIndex::default(),
                            restart_offset: 0,
                            num_restarts: 0,
                            global_seqno,
                        }
                    } else {
                        Block {
                            restart_offset,
                            num_restarts,
                            global_seqno,
                            data,
                            data_block_hash_index: DataBlockHashIndex::default(),
                        }
                    }
                }
                DataBlockIndexType::DataBlockBinaryAndHash => {
                    unimplemented!();
                }
            }
        }
    }
}

pub fn calculate_num_restarts(data: &[u8]) -> u32 {
    let offset = data.len() - std::mem::size_of::<u32>();
    let block_footer = decode_fixed_uint32(&data[offset..]);
    if data.len() > MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX {
        block_footer
    } else {
        let (_, num_restarts) = un_pack_index_type_and_num_restarts(block_footer);
        num_restarts
    }
}

pub fn calculate_index_type(data: &[u8]) -> DataBlockIndexType {
    if data.len() > MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX {
        return DataBlockIndexType::DataBlockBinarySearch;
    }
    let offset = data.len() - std::mem::size_of::<u32>();
    let block_footer = decode_fixed_uint32(&data[offset..]);
    let (tp, _) = un_pack_index_type_and_num_restarts(block_footer);
    tp
}

pub fn un_pack_index_type_and_num_restarts(block_footer: u32) -> (DataBlockIndexType, u32) {
    let tp = if block_footer & (1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT) {
        DataBlockIndexType::DataBlockBinaryAndHash
    } else {
        DataBlockIndexType::DataBlockBinarySearch
    };
    (tp, block_footer & NUM_RESTARTS_MASK)
}

pub fn pack_index_type_and_num_restarts(tp: DataBlockIndexType, num_restarts: u32) -> u32 {
    let mut footer = num_restarts;
    if tp == DataBlockIndexType::DataBlockBinaryAndHash {
        footer |= 1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT;
    }
    footer
}

pub struct DataBlockIter<'a> {
    data: &'a [u8],
    restart_offset: usize,
    comparator: Arc<dyn KeyComparator>,
    current: u32,
    num_restars: u32,
}

impl DataBlockIter {
    fn get_restart_point(&self, index: u32) -> u32{
        let offset = self.restart_offset  + index as usize * std::mem::size_of::<u32>();
        decode_fixed_uint32(&self.data[offset..])
    }

    fn binary_seek_index(&self, key: &[u8])-> i32 {
        let mut left = 0;
        let mut right = self.num_restars - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);
        }
        -1
    }

    pub fn seek(&mut self, key: &[u8]) {
    }
    pub fn seek_to_first(&mut self) {}
    pub fn next(&mut self) {}
    pub fn prev(&mut self) {
        unimplemented!();
    }
    pub fn key(&self) -> &[u8] {}
    pub fn value(&self) -> &[u8] {}
    pub fn valid(&self) -> bool {
    }
}
