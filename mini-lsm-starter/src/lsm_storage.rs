#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod mini_lsm;
mod options;
mod tables;

pub use self::mini_lsm::MiniLsm;
pub use self::options::LsmStorageOptions;
pub use self::tables::LsmStorageTables;

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController,
    SimpleLeveledCompactionController, TieredCompactionController,
};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorage {
    /// 使用读写锁来操作LsmStorage的tables.
    ///
    /// 将tables的类型从Arc<RwLock<<ArcLsmStorageTables>>>
    /// 修改为Arc<RwLock<LsmStorageTables>>支持能原地修改
    /// LsmStorageTables里的值, 从而避免拷贝.
    pub(crate) tables: Arc<RwLock<LsmStorageTables>>,
    pub(crate) tables_lock: Mutex<()>,

    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

impl LsmStorage {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageTables::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            tables: Arc::new(RwLock::new(state)),
            tables_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    ///
    /// 从存储中获取key对应的值. 在第7天时, 可以使用布隆过滤器来进一步优化.<br>
    /// 先从memtable中读, 不存在再从immutable_memtable中读.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let tables = self.tables.read();
        // find_value的返回值是Option<Option<Bytes>.
        // 1. 第1层的Option表示是否查找到了key.
        // 2. 第2层的Option表示查找的key有效.
        let find_value = |table: &MemTable| {
            table
                .get(key)
                .map(|value| if value.is_empty() { None } else { Some(value) })
        };

        // 1. 从memtable中查找, 当key值为None时被删.
        if let Some(value) = find_value(&tables.memtable) {
            return Ok(value);
        }

        // 2. memtable没有, 从imm_memtable中查找.
        tables
            .imm_memtables
            .iter()
            .find_map(|table| find_value(table))
            .map_or(Ok(None), Ok)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    ///
    /// 通过写入当前memtable来将key-value对放到存储中. 当memtable的大小超过
    /// limit时, 需要将当前memtable冻结为immutable的.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size;
        {
            // 1. 将key-value写入到memtable.
            let tables = self.tables.write();
            tables.memtable.put(key, value)?;
            size = tables.memtable.approximate_size();
        }

        // 2. 尝试冻结memtable, 可能会没达到limit
        self.try_freeze_memtable(size)?;
        Ok(())
    }

    /// 判断当前memtable是否达到limit, 尝试冻结memtable.
    fn try_freeze_memtable(&self, approximate_size: usize) -> Result<()> {
        let size_limit = self.options.target_sst_size;
        if approximate_size >= size_limit {
            let lock = self.tables_lock.lock();
            let tables = self.tables.read();

            // 对tables_lock加锁之后, 再次判断, 确保只有1个线程执行.
            if tables.memtable.approximate_size() >= size_limit {
                drop(tables); // 释放读锁, 避免加写锁造成死锁
                self.force_freeze_memtable(&lock)?
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    ///
    /// 通过写入空值从存储中删除1个key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable.
    ///
    /// 强制将当前memtable冻结为不可变memtable. tables的读写锁在外层必须释放掉,
    /// 否则会造成死锁. _state_lock_observer确保在外层已加锁, 内部保持有效.
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // 1. 创建新的memtable
        let id = self.next_sst_id();
        let memtable = Arc::new(MemTable::create(id));
        {
            // 2. 对memtable加锁, 将memtable冻结为immutable
            let mut tables = self.tables.write();
            let old_memtable = Arc::clone(&tables.memtable);
            tables.imm_memtables.insert(0, old_memtable);
            tables.memtable = Arc::clone(&memtable);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        unimplemented!()
    }
}
