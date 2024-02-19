#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

#[derive(PartialEq, Eq)]
pub enum LogRecordType {
    // 正常记录
    NORMAL = 1,
    // 删除记录
    DELETE = 2,
}

/// 写入到日志文件的记录
/// 之所以叫日志，是因为数据文件中的数据是追加写入的
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }
}

// 从数据文件中读取的记录
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}
