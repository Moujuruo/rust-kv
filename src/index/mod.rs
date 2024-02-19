pub mod btree;

use crate::data::log_record::LogRecordPos;
use crate::options::IndexType;

pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
}

/// 根据配置创建索引
pub fn create_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
        _ => panic!("Unsupported index type"),
    }
}
