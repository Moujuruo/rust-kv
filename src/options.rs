use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    // 数据库目录
    pub dir_path: PathBuf,
    // 数据库文件大小
    pub file_size: u64,
    // 是否每次写都持久化
    pub sync: bool,
    // 索引类型
    pub index_type: IndexType,
}

#[derive(Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}
