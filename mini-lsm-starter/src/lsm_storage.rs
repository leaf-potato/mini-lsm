#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod mini_lsm;
mod options;
mod tables;

pub(crate) use self::mini_lsm::LsmStorageInner;
pub use self::mini_lsm::MiniLsm;
pub use self::options::LsmStorageOptions;
pub use self::tables::LsmStorageTables;

use std::sync::Arc;

use bytes::Bytes;

use crate::block::Block;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}
