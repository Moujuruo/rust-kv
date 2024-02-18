use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index,
    options::Options,
};

/// 存储引擎实例
pub struct Engine {
    options: Arc<Options>,                            // 配置
    active_file: Arc<RwLock<DataFile>>,               // 活跃数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧数据文件
    index: Box<dyn index::Indexer>,                   // 内存索引
}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造 LogRecord
        let mut logrecord = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };

        // 追加写到活跃数据文件中
        let log_record_pos = self.append_log_record(&mut logrecord)?;

        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::IndexUpdateError);
        }

        Ok(())
    }

    /// 获取 key 对应的 value
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中查找
        let log_record_pos = self.index.get(key.to_vec());
        if log_record_pos.is_none() {
            return Err(Errors::RecordNotFound);
        }

        // 从数据文件中读取 LogRecord
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let logrecord = match active_file.get_file_id() == log_record_pos.unwrap().file_id {
            true => active_file.read_log_record(log_record_pos.unwrap().offset)?,
            false => {
                let data_file = older_files.get(&log_record_pos.unwrap().file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.unwrap().offset)?
            }
        };

        // 判断类型
        if logrecord.record_type == LogRecordType::DELETE {
            return Err(Errors::RecordNotFound);
        }

        // 返回 value
        Ok(logrecord.value.into())
    }

    /// 追加写数据到当前活跃文件中
    pub fn append_log_record(&self, logrecord: &mut LogRecord) -> Result<LogRecordPos> {
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

        // 追加写到活跃数据文件中
        let write_offset = active_file_guard.get_write_offset();
        active_file_guard.write(&encoded)?;

        // 根据配置项决定是否立即持久化
        if self.options.sync {
            active_file_guard.sync()?;
        }

        // 构造数据索引信息
        Ok(LogRecordPos {
            file_id: active_file_guard.get_file_id(),
            offset: write_offset,
        })
    }
}
