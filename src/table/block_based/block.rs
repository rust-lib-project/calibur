use crate::common::format::{extract_user_key, GlobalSeqnoAppliedKey, Slice};
use crate::common::{
    BufferedFileReader, CompressionType, Error, KeyComparator, RandomAccessFileReader, Result,
};
use crate::table::block_based::compression::{CompressionAlgorithm, UncompressionInfo};
use crate::table::block_based::data_block_hash_index_builder::DataBlockHashIndex;
use crate::table::block_based::lz4::LZ4CompressionAlgorithm;
use crate::table::block_based::options::DataBlockIndexType;
use crate::table::block_based::BLOCK_TRAILER_SIZE;
use crate::table::format::{BlockHandle, IndexValue, MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX};
use crate::table::InternalIterator;
use crate::util::{decode_fixed_uint32, get_var_uint32};
use std::cmp::Ordering;
use std::sync::Arc;

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
            Block {
                data: vec![],
                restart_offset: 0,
                num_restarts: 0,
                global_seqno,
                data_block_hash_index: DataBlockHashIndex::default(),
            }
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

    pub fn new_data_iterator(self: Arc<Self>, comparator: Arc<dyn KeyComparator>) -> DataBlockIter {
        DataBlockIter::new(
            self.restart_offset,
            self.num_restarts,
            self.global_seqno,
            self,
            false,
            comparator,
        )
    }

    // TODO: support encode index key without seq
    pub fn new_index_iterator(
        self: &Arc<Self>,
        comparator: Arc<dyn KeyComparator>,
        key_includes_seq: bool,
    ) -> IndexBlockIter {
        let inner = self.clone().new_data_iterator(comparator);
        IndexBlockIter {
            inner,
            decoded_value: Default::default(),
            key_includes_seq,
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

pub struct DataBlockIter {
    block: Arc<Block>,
    restart_offset: usize,
    comparator: Arc<dyn KeyComparator>,
    current: usize,
    num_restarts: u32,
    restart_index: u32,
    applied_key: GlobalSeqnoAppliedKey,
    value: Slice,
}

impl InternalIterator for DataBlockIter {
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
        self.seek_to_restart_point(self.num_restarts - 1);
        while self.parse_next_key() && self.value.limit < self.restart_offset {}
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        let index = self.binary_seek_index(key);
        if index == -1 {
            self.current = self.restart_offset;
            return;
        }
        self.seek_to_restart_point(index as u32);
        while self.parse_next_key() && self.comparator.less_than(self.applied_key.get_key(), key) {}
        if !self.valid() {
            self.seek_to_last();
        } else {
            while self.valid() && self.comparator.less_than(key, self.applied_key.get_key()) {
                self.prev();
            }
        }
    }

    fn next(&mut self) {
        self.parse_next_key();
    }

    fn prev(&mut self) {
        let original = self.current;
        while self.get_restart_point(self.restart_index) as usize >= original {
            if self.restart_index == 0 {
                self.current = self.restart_offset;
                self.restart_index = self.num_restarts;
                return;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart_point(self.restart_index);
        while self.parse_next_key() {
            if self.value.limit >= original {
                break;
            }
        }
    }

    fn key(&self) -> &[u8] {
        self.applied_key.get_key()
    }

    fn value(&self) -> &[u8] {
        &self.block.data[self.value.offset..self.value.limit]
    }
}

impl DataBlockIter {
    pub fn new(
        restart_offset: usize,
        num_restarts: u32,
        global_seqno: u64,
        block: Arc<Block>,
        is_user_key: bool,
        comparator: Arc<dyn KeyComparator>,
    ) -> Self {
        Self {
            block,
            comparator,
            restart_offset,
            current: 0,
            num_restarts,
            restart_index: 0,
            applied_key: GlobalSeqnoAppliedKey::new(global_seqno, is_user_key),
            value: Slice::default(),
        }
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        let offset = self.restart_offset + index as usize * std::mem::size_of::<u32>();
        decode_fixed_uint32(&self.block.data[offset..])
    }

    fn binary_seek_index(&mut self, key: &[u8]) -> i32 {
        let mut left = 0;
        let mut right = self.num_restarts - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid) as usize;
            let (next_offset, shared, non_shared) = decode_key(&self.block.data[region_offset..]);
            assert_eq!(shared, 0);
            if next_offset == 0 {
                return -1;
            }
            let start = region_offset + next_offset;
            let limit = start + non_shared as usize;
            self.applied_key.set_key(&self.block.data[start..limit]);
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
        self.get_restart_point(index);
        self.value.offset = self.get_restart_point(index) as usize;
        self.value.limit = self.value.offset;
    }

    fn parse_next_key(&mut self) -> bool {
        self.current = self.value.limit;
        if self.current >= self.restart_offset {
            return false;
        }
        let (offset, shared, non_shared, val_len) = decode_entry(&self.block.data[self.current..]);
        if offset == 0 {
            return false;
        }

        let current = self.current + offset;
        self.value.offset = current + non_shared as usize;
        self.value.limit = self.value.offset + val_len as usize;
        if shared == 0 {
            self.applied_key
                .set_key(&self.block.data[current..self.value.offset]);
        } else {
            self.applied_key.trim_append(
                &self.block.data[current..self.value.offset],
                shared as usize,
            );
        }
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

pub struct IndexBlockIter {
    inner: DataBlockIter,
    decoded_value: IndexValue,
    key_includes_seq: bool,
}

impl IndexBlockIter {
    fn update_current_key(&mut self) {
        let _ = self.decoded_value.decode_from(self.inner.value());
    }

    pub fn index_value(&self) -> &IndexValue {
        &self.decoded_value
    }
}

impl InternalIterator for IndexBlockIter {
    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn seek(&mut self, key: &[u8]) {
        if self.key_includes_seq {
            self.inner.seek(key);
        } else {
            self.inner.seek(extract_user_key(key));
        }
        self.update_current_key();
    }

    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.update_current_key();
    }

    fn seek_to_last(&mut self) {
        self.inner.seek_to_last();
        self.update_current_key();
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        if self.key_includes_seq {
            self.inner.seek_for_prev(key);
        } else {
            self.inner.seek_for_prev(extract_user_key(key));
        }
        self.update_current_key();
    }

    fn next(&mut self) {
        self.inner.next();
        self.update_current_key();
    }

    fn prev(&mut self) {
        self.inner.prev();
        self.update_current_key();
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
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
    let mut offset = 0;
    let shared = match get_var_uint32(key, &mut offset) {
        Some(val) => val,
        None => return (0, 0, 0, 0),
    };
    let non_shared = match get_var_uint32(&key[offset..], &mut offset) {
        Some(val) => val,
        None => return (0, 0, 0, 0),
    };
    let val_len = match get_var_uint32(&key[offset..], &mut offset) {
        Some(val) => val,
        None => return (0, 0, 0, 0),
    };
    (offset, shared, non_shared, val_len)
}

pub async fn read_block_from_file(
    file: &RandomAccessFileReader,
    handle: &BlockHandle,
    global_seqno: u64,
) -> Result<Arc<Block>> {
    let read_len = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut data = vec![0u8; read_len];
    file.read_exact(handle.offset as usize, read_len, data.as_mut_slice())
        .await?;
    let compression_type: CompressionType = data[handle.size as usize].into();
    match compression_type {
        CompressionType::LZ4Compression => {
            let lz4 = LZ4CompressionAlgorithm {};
            let info = UncompressionInfo {};
            let data = lz4.uncompress(&info, 2, &data[..handle.size as usize])?;
            Ok(Arc::new(Block::new(data, global_seqno)))
        }
        CompressionType::NoCompression => {
            data.resize(handle.size as usize, 0);
            Ok(Arc::new(Block::new(data, global_seqno)))
        }
        _ => Err(Error::Unsupported(format!(
            "compression type: {:?}",
            compression_type
        ))),
    }
}

pub async fn read_block_from_buffered_file(
    file: &BufferedFileReader,
    handle: &BlockHandle,
) -> Result<Vec<u8>> {
    let read_len = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut data = vec![0u8; read_len];
    file.read(handle.offset as usize, data.as_mut_slice())
        .await?;
    let compression_type: CompressionType = data[handle.size as usize].into();
    match compression_type {
        CompressionType::LZ4Compression => {
            let lz4 = LZ4CompressionAlgorithm {};
            let info = UncompressionInfo {};
            lz4.uncompress(&info, 2, &data[..handle.size as usize])
        }
        CompressionType::NoCompression => {
            data.resize(handle.size as usize, 0);
            Ok(data)
        }
        _ => Err(Error::Unsupported(format!(
            "compression type: {:?}",
            compression_type
        ))),
    }
}
