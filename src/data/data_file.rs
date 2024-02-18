use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;

use crate::{errors::Result, fio::IOManager};

use super::log_record::LogRecord;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    pub file_id: Arc<RwLock<u32>>,      // 文件 ID
    pub write_offset: Arc<RwLock<u64>>, // 写入偏移, 记录当前写到了文件的哪个位置
    pub io_manager: Box<dyn IOManager>, // IO 管理器
}

impl DataFile {
    /// 创建或打开一个数据文件
    pub fn new(file_id: u32, dir_path: PathBuf) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_offset(&self) -> u64 {
        let write_offset_guard = self.write_offset.read();
        *write_offset_guard
    }

    pub fn get_file_id(&self) -> u32 {
        let file_id_guard = self.file_id.read();
        *file_id_guard
    }

    /// 读取日志记录
    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
