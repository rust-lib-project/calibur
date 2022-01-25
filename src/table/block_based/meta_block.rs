use crate::common::format::BlockHandle;
use crate::table::block_based::block_builder::BlockBuilder;
use crate::table::block_based::options::{DataBlockIndexType, IndexType};
use crate::table::table_properties::TableProperties;
use crate::util::encode_var_uint64;
use std::collections::HashMap;

pub struct MetaIndexBuilder {
    meta_index_block: BlockBuilder,
    meta_block_handles: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MetaIndexBuilder {
    pub fn new() -> MetaIndexBuilder {
        MetaIndexBuilder {
            meta_block_handles: vec![],
            meta_index_block: BlockBuilder::new(
                1usize,
                true,
                DataBlockIndexType::DataBlockBinarySearch,
                0.75,
            ),
        }
    }

    pub fn add(&mut self, key: &[u8], handle: &BlockHandle) {
        let mut handle_encoding = Vec::with_capacity(20);
        handle.encode_to(&mut handle_encoding);
        self.meta_block_handles
            .push((key.to_vec(), handle_encoding));
    }

    pub fn finish(&mut self) -> &[u8] {
        self.meta_block_handles.sort_by(|x, y| x.0.cmp(&y.0));
        for (k, v) in self.meta_block_handles.iter() {
            self.meta_index_block.add(k.as_slice(), v.as_slice());
        }
        self.meta_index_block.finish()
    }
}

pub struct PropertyBlockBuilder {
    properties_block: BlockBuilder,
    props: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PropertyBlockBuilder {
    pub fn new() -> PropertyBlockBuilder {
        PropertyBlockBuilder {
            props: vec![],
            properties_block: BlockBuilder::new(
                u32::MAX as usize,
                true,
                DataBlockIndexType::DataBlockBinarySearch,
                0.75,
            ),
        }
    }

    pub fn add_u64(&mut self, key: &[u8], val: u64) {
        let mut tmp: [u8; 10] = [0u8; 10];
        encode_var_uint64(&mut tmp, val);
        self.add_key(key, &tmp);
    }

    pub fn add_key(&mut self, key: &[u8], val: &[u8]) {
        self.props.push((key.to_vec(), val.to_vec()));
    }

    pub fn add_properties(
        &mut self,
        key: &[u8],
        user_collected_properties: &HashMap<String, Vec<u8>>,
    ) {
        for (k, v) in user_collected_properties {
            self.props.push((k.clone().into_bytes(), v.clone()));
        }
    }

    pub fn add_table_properties(&mut self, props: &TableProperties) {}

    pub fn finish(&mut self) -> &[u8] {
        self.props.sort_by(|x, y| x.0.cmp(&y.0));
        for (k, v) in self.props.iter() {
            self.properties_block.add(k.as_slice(), v.as_slice());
        }
        self.properties_block.finish()
    }
}
