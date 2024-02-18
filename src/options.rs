use std::path::PathBuf;



pub struct Options {
    // 数据库目录
    pub dir_path: PathBuf,
    // 数据库文件大小
    pub file_size: u64,
}