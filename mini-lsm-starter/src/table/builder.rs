use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    ongoing_block_builder: BlockBuilder,
    first_key: Vec<u8>,
    data: Vec<u8>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            meta: Vec::new(),
            ongoing_block_builder: BlockBuilder::new(block_size),
            data: Vec::new(),
            block_size,
            first_key: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }
        // try to add to current block
        if self.ongoing_block_builder.add(key, value) {
            return;
        }
        self.finish_block();
        debug_assert!(self.ongoing_block_builder.add(key, value));
        self.first_key = key.to_vec();
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // finish building ongoing block
        self.finish_block();

        let block_meta_offset = self.data.len();

        let mut buffer = std::mem::take(&mut self.data);

        BlockMeta::encode_block_meta(&self.meta, &mut buffer);

        buffer.put_u32(block_meta_offset as u32);

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), buffer).unwrap(),
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    /// utility function to finish building the current block
    fn finish_block(&mut self) {
        // builds new block meta and reset current first key in a single pass to prepare for new block
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into(),
        });
        // reset block builder in self and take the one ready to build out of it to get mutable access.
        let ready_builder = std::mem::replace(
            &mut self.ongoing_block_builder,
            BlockBuilder::new(self.block_size),
        );
        let encoded_data = ready_builder.build().encode();
        self.data.extend(encoded_data);
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
