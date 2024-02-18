use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{data::{data_file::DataFile, log_record::{LogRecord, LogRecordType}}, errors::{Errors, Result}, options::Options};


/// 存储引擎实例
pub struct Engine {
    options: Arc<Options>, // 配置
    active_file: Arc<RwLock<DataFile>>, // 活跃数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧数据文件
}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        
        // 构造 LogRecord
        let logrecord = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };

        // 追加写到活跃数据文件中

        Ok(())
    }

    /// 追加写数据到当前活跃文件中
    pub fn append_log_record(&self, logrecord: &mut LogRecord) -> Result<()> {
        // 追加写到活跃数据文件中
        let dirpath = self.options.dir_path.clone();

        // 编码 LogRecord
        let encoded = logrecord.encode();
        let log_size = encoded.len() as u64;

        // 获取当前活跃文件
        let mut active_file_guard = self.active_file.write();

        // 判断是否需要切换文件
        let write_offset = active_file_guard.get_write_offset();
        if write_offset + log_size > self.options.file_size {
            // 将当前活跃文件持久化
            active_file_guard.sync()?;
            let cur_file_id = active_file_guard.get_file_id();
            // 旧数据文件存储到 Map
            let mut older_files_guard = self.older_files.write();
            let old_file = DataFile::new(cur_file_id, dirpath.clone())?;
            older_files_guard.insert(cur_file_id, old_file);
            
            // 创建新的活跃文件
            let new_file = DataFile::new(cur_file_id + 1, dirpath)?;
            *active_file_guard = new_file;
            
        }

        Ok(())
    }
}