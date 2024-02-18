use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;

use crate::{errors::Result, fio::IOManager};

pub struct DataFile {
    pub file_id: Arc<RwLock<u32>>, // 文件 ID
    pub write_offset: Arc<RwLock<u64>>, // 写入偏移, 记录当前写到了文件的哪个位置
    pub io_manager: Box<dyn IOManager>, // IO 管理器
}

impl DataFile {
    // 创建或打开一个数据文件
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

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}