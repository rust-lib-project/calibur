use crate::common::format::{pack_sequence_and_type, Slice};
use crate::common::{KeyComparator, DISABLE_GLOBAL_SEQUENCE_NUMBER, RandomAccessFileReader, Result};
use crate::compactor::Compactor;
use crate::table::block_based::data_block_hash_index_builder::DataBlockHashIndex;
use crate::table::block_based::options::DataBlockIndexType;
use crate::table::format::{BlockHandle, MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX};
use crate::util::{decode_fixed_uint32, decode_fixed_uint64, get_var_uint32};
use std::cmp::Ordering;
use std::sync::Arc;
use crate::table::block_based::BLOCK_TRAILER_SIZE;
use crate::table::InternalIterator;

const DATA_BLOCK_INDEX_TYPE_BIT_SHIFT: u32 = 31;

const MAX_NUM_RESTARTS: u32 = (1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT) - 1u32;

// 0x7FFFFFFF
const NUM_RESTARTS_MASK: u32 = (1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT) - 1u32;

pub struct Block {
    pub data: Vec<u8>,
    pub restart_offset: usize,
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
                    let restart_offset = data.len() as usize
                        - (1 + num_restarts as usize) * std::mem::size_of::<u32>();
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

    pub fn new_data_iterator(&self, comparator: Arc<dyn KeyComparator>) -> DataBlockIter {
        DataBlockIter::new(
            &self.data,
            self.restart_offset,
            self.num_restarts,
            self.global_seqno,
            comparator,
        )
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
    let tp = if (block_footer & (1u32 << DATA_BLOCK_INDEX_TYPE_BIT_SHIFT)) > 0 {
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

pub struct GlobalSeqnoAppliedKey {
    offset: usize,
    limit: usize,
    internal_key: Vec<u8>,
    global_seqno: u64,
}

impl GlobalSeqnoAppliedKey {
    fn get_key(&self) -> &[u8] {
        &self.internal_key
    }

    fn set_key(&mut self, data: &[u8], offset: usize, key_len: usize) {
        if self.global_seqno == DISABLE_GLOBAL_SEQUENCE_NUMBER {
            self.internal_key.clear();
            self.internal_key
                .extend_from_slice(&data[offset..(offset + key_len)]);
            return;
        }
        let tail_offset = offset + key_len - 8;
        let num = decode_fixed_uint64(&data[tail_offset..]);
        self.internal_key
            .extend_from_slice(&data[offset..tail_offset]);
        let num = pack_sequence_and_type(self.global_seqno, (num & 0xff) as u8);
        self.internal_key.extend_from_slice(&num.to_le_bytes());
    }

    fn trim_append(&mut self, data: &[u8], offset: usize, shared: usize, non_shared: usize) {
        self.internal_key.resize(shared, 0);
        self.internal_key
            .extend_from_slice(&data[offset..(offset + non_shared)]);
    }
}

pub struct DataBlockIter<'a> {
    data: &'a [u8],
    restart_offset: usize,
    comparator: Arc<dyn KeyComparator>,
    current: usize,
    num_restarts: u32,
    restart_index: u32,
    applied_key: GlobalSeqnoAppliedKey,
    value: Slice,
}

impl <'a> InternalIterator for DataBlockIter<'a> {
    fn valid(&self) -> bool {
        self.current < self.restart_offset
    }
    fn seek(&mut self, key: &[u8]) {
        let index = self.binary_seek_index(key);
        if index == -1 {
            self.current = self.restart_offset;
            return;
        }
        self.seek_to_restart_point(index as u32);
        while self.parse_next_key() && self.comparator.less_than(self.applied_key.get_key(), key) {}
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_key();
    }

    fn seek_to_last(&mut self) {
        unimplemented!()
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        unimplemented!()
    }

    fn next(&mut self) {
        self.parse_next_key();
    }

    fn prev(&mut self) {
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        self.applied_key.get_key()
    }

    fn value(&self) -> &[u8] {
        &self.data[self.value.offset..self.value.limit]
    }
}

impl<'a> DataBlockIter<'a> {
    pub fn new(
        data: &'a [u8],
        restart_offset: usize,
        num_restarts: u32,
        global_seqno: u64,
        comparator: Arc<dyn KeyComparator>,
    ) -> Self {
        Self {
            data,
            comparator,
            restart_offset,
            current: 0,
            num_restarts,
            restart_index: 0,
            applied_key: GlobalSeqnoAppliedKey {
                global_seqno,
                internal_key: vec![],
                offset: 0,
                limit: 0,
            },
            value: Slice::default(),
        }
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        let offset = self.restart_offset + index as usize * std::mem::size_of::<u32>();
        decode_fixed_uint32(&self.data[offset..])
    }

    fn binary_seek_index(&mut self, key: &[u8]) -> i32 {
        let mut left = 0;
        let mut right = self.num_restarts - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid) as usize;
            let (next_offset, shared, non_shared) = decode_key(&self.data[region_offset..]);
            if next_offset == 0 {
                return -1;
            }
            self.applied_key
                .set_key(self.data, region_offset + next_offset, non_shared as usize);
            let ord = self.comparator.compare_key(self.applied_key.get_key(), key);
            if ord == Ordering::Less {
                left = mid;
            } else if ord == Ordering::Greater {
                right = mid - 1;
            } else {
                left = mid;
                right = mid;
            }
        }
        left as i32
    }
    fn seek_to_restart_point(&mut self, index: u32) {
        self.restart_index = index;
        self.get_restart_point(index) as usize;
        self.value.offset = self.get_restart_point(index) as usize;
        self.value.limit = self.value.offset;
    }

    fn parse_next_key(&mut self) -> bool {
        self.current = self.value.limit;
        if self.current >= self.restart_offset {
            return false;
        }
        let (offset, shared, non_shared, val_len) = decode_entry(&self.data[self.current..]);
        if offset == 0 {
            return false;
        }

        let current = self.current + offset;
        if shared == 0 {
            self.applied_key
                .set_key(self.data, current, non_shared as usize);
        } else {
            self.applied_key
                .trim_append(self.data, current, shared as usize, non_shared as usize);
        }
        self.value.offset = current + non_shared as usize;
        self.value.limit = self.value.offset + val_len as usize;
        if shared == 0 {
            while self.restart_index + 1 < self.num_restarts
                && self.get_restart_point(self.restart_index + 1) < self.current as u32
            {
                self.restart_index += 1;
            }
        }
        true
    }

}

pub fn decode_key(key: &[u8]) -> (usize, u32, u32) {
    let (offset, shared, non_shared, _) = decode_entry(key);
    (offset, shared, non_shared)
}

pub fn decode_entry(key: &[u8]) -> (usize, u32, u32, u32) {
    if (key[0] | key[1] | key[2]) < 128 {
        return (3, key[0] as u32, key[1] as u32, key[2] as u32);
    }
    let (next_offset, shared) = match get_var_uint32(key) {
        Some((offset, val)) => (offset, val),
        None => return (0, 0, 0, 0),
    };
    let (next_offset, non_shared) = match get_var_uint32(&key[next_offset..]) {
        Some((offset, val)) => (next_offset + offset, val),
        None => return (0, 0, 0, 0),
    };
    let (next_offset, val_len) = match get_var_uint32(&key[next_offset..]) {
        Some((offset, val)) => (next_offset + offset, val),
        None => return (0, 0, 0, 0),
    };
    (next_offset, shared, non_shared, val_len)
}

pub async fn read_block_from_file(
    file: &RandomAccessFileReader,
    handle: &BlockHandle,
    global_seqno: u64,
) -> Result<Box<Block>> {
    let read_len = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut data = vec![0u8; read_len];
    file.read(handle.offset as usize, read_len, data.as_mut_slice())
        .await?;
    // TODO: uncompress block
    Ok(Box::new(Block::new(data, global_seqno)))
}