use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::data::log_record::LogRecordPos;

use super::Indexer;

// BTree 索引, 主要封装了标准库的 BTreeMap 结构
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        BTree {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }
    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let result = write_guard.remove(&key);
        result.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put() {
        let btree = BTree::new();
        let result1 = btree.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(result1, true);

        let result2 = btree.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 20,
            },
        );
        assert_eq!(result2, true);
    }

    #[test]
    fn test_get() {
        let btree = BTree::new();
        btree.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        btree.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 20,
            },
        );

        let result1 = btree.get("".as_bytes().to_vec());
        assert_eq!(
            result1,
            Some(LogRecordPos {
                file_id: 1,
                offset: 10
            })
        );

        let result2 = btree.get("aa".as_bytes().to_vec());
        assert_eq!(
            result2,
            Some(LogRecordPos {
                file_id: 2,
                offset: 20
            })
        );
    }

    #[test]
    fn test_delete() {
        let btree = BTree::new();
        btree.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        btree.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 20,
            },
        );

        let result1 = btree.delete("".as_bytes().to_vec());
        assert_eq!(result1, true);

        let result2 = btree.delete("aa".as_bytes().to_vec());
        assert_eq!(result2, true);

        let res1 = btree.get("".as_bytes().to_vec());
        assert_eq!(res1, None);

        let res2 = btree.get("aa".as_bytes().to_vec());
        assert_eq!(res2, None);

        let result3 = btree.delete("not exist".as_bytes().to_vec());
        assert_eq!(result3, false);
    }
}
