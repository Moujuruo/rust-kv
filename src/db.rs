use std::{collections::HashMap, fs, path::PathBuf, sync::Arc, u32};

use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index,
    options::Options,
};

const INITIAL_FILE_ID: u32 = 0;

/// 存储引擎实例
pub struct Engine {
    options: Arc<Options>,                            // 配置
    active_file: Arc<RwLock<DataFile>>,               // 活跃数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧数据文件
    index: Box<dyn index::Indexer>,                   // 内存索引
    files_id: Vec<u32>,                               // 文件 ID，只在初始化时使用
}

impl Engine {
    /// 打开一个存储引擎实例
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }
        let options = opts.clone();

        // 判断数据目录是否存在，不存在则创建
        if !opts.dir_path.exists() {
            let create_res = fs::create_dir(opts.dir_path.clone());
            if create_res.is_err() {
                warn!(
                    "failed to create database dir: {:?}",
                    create_res.unwrap_err()
                );
                return Err(Errors::FailedToCreateDataBaseDir);
            }
        }

        // 从目录中读取数据文件
        let mut data_files = load_data_files(opts.dir_path.clone())?;
        // 设置 file_id 信息
        let mut files_id: Vec<u32> = Vec::new();
        for file in data_files.iter() {
            files_id.push(file.get_file_id());
        }

        // 将旧的数据文件保存到 older_files 中
        let mut older_files: HashMap<u32, DataFile> = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // 拿到活跃数据文件
        let active_file = match data_files.pop() {
            Some(file) => file,
            None => DataFile::new(INITIAL_FILE_ID, opts.dir_path.clone())?,
        };

        let engine = Engine {
            options: Arc::new(options),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::create_indexer(opts.index_type)),
            files_id,
        };

        // 从数据文件中加载内存索引
        engine.load_index_from_data_files()?;

        Ok(engine)
    }

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

    // 删除 key 对应的数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中查找
        let log_record_pos = self.index.get(key.to_vec());
        if log_record_pos.is_none() {
            return Ok(());
        }

        // 构造 LogRecord
        let mut logrecord = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            record_type: LogRecordType::DELETE,
        };

        self.append_log_record(&mut logrecord)?;

        let ok = self.index.delete(key.to_vec());
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
        if logrecord.record.record_type == LogRecordType::DELETE {
            return Err(Errors::RecordNotFound);
        }

        // 返回 value
        Ok(logrecord.record.value.into())
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

    /// 从数据文件中加载内存索引
    pub fn load_index_from_data_files(&self) -> Result<()> {
        // 数据文件为空，直接返回
        if self.files_id.is_empty() {
            return Ok(());
        }

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 遍历每个文件 id
        for (i, file_id) in self.files_id.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // 构建索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                let ok = match log_record.record_type {
                    LogRecordType::NORMAL => {
                        self.index.put(log_record.key.to_vec(), log_record_pos)
                    }
                    LogRecordType::DELETE => self.index.delete(log_record.key.to_vec()),
                };
                if !ok {
                    return Err(Errors::IndexUpdateError);
                }

                offset += size;
            }
            // 设置活跃文件的写入偏移
            if i == self.files_id.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }
        Ok(())
    }
}

// 从目录中读取数据文件
fn load_data_files(dir_path: PathBuf) -> Result<Vec<DataFile>> {
    let mut dir_files: Vec<DataFile> = Vec::new();
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDataBaseDir);
    }
    let mut files_id: Vec<u32> = Vec::new();
    for entry in dir.unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            // 拿到文件名
            let file_name = path.file_name().unwrap().to_str().unwrap();
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                // 解析文件 ID，文件名格式为 {id}.data
                let file_id = file_name
                    .trim_end_matches(DATA_FILE_NAME_SUFFIX)
                    .parse::<u32>();
                if file_id.is_err() {
                    return Err(Errors::DataDirectoryInvalid);
                }
                files_id.push(file_id.unwrap());
            }
        }
    }
    // 判空
    if files_id.is_empty() {
        return Ok(dir_files);
    }
    files_id.sort();
    // 遍历文件 ID，加载数据文件
    for file_id in files_id {
        let file = DataFile::new(file_id, dir_path.clone())?;
        dir_files.push(file);
    }
    Ok(dir_files)
}

// 检查配置项
fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.file_size <= 0 {
        return Some(Errors::FileSizeTooSmall);
    }
    None
}
