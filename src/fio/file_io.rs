use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::Arc,
};

use parking_lot::RwLock;

use super::IOManager;

use log::error;

use crate::errors::{Errors, Result};

pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_name)
        {
            Ok(fd) => Ok(FileIO {
                fd: Arc::new(RwLock::new(fd)),
            }),
            Err(e) => {
                error!("open file error: {}", e);
                return Err(Errors::FailedToOpenDataFile);
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read file error: {}", e);
                return Err(Errors::FailedToReadFromDataFile);
            }
        };
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("write file error: {}", e);
                return Err(Errors::FailedToWriteToDataFile);
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        match read_guard.sync_all() {
            Ok(_) => return Ok(()),
            Err(e) => {
                error!("sync file error: {}", e);
                return Err(Errors::FailedToSyncDataFile);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::*;

    #[test]
    fn test_write() {
        let path = PathBuf::from("test_write.txt");
        let file_io = FileIO::new(path.clone());
        assert!(file_io.is_ok());
        let fio = file_io.unwrap();
        let result = fio.write("hello world!!Q".as_bytes());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 14);

        let res3 = fs::remove_file(path);
        assert!(res3.is_ok());
    }

    #[test]
    fn test_read() {
        let path = PathBuf::from("test_read.txt");
        let file_io = FileIO::new(path.clone());
        assert!(file_io.is_ok());
        let fio = file_io.unwrap();
        let result = fio.write("hello world!!Q".as_bytes());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 14);

        let mut buf = [0u8; 14]; // 修改缓冲区大小为14
        let res = fio.read(&mut buf, 0);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 14); // 读取的字节数应为14
        assert_eq!(buf, "hello world!!Q".as_bytes());

        let res3 = fs::remove_file(path);
        assert!(res3.is_ok());
    }

    #[test]
    fn test_sync() {
        let path = PathBuf::from("test_sync.txt");
        let file_io = FileIO::new(path.clone());
        assert!(file_io.is_ok());
        let fio = file_io.unwrap();
        let result = fio.write("hello world!!Q".as_bytes());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 14);

        let res = fio.sync();
        assert!(res.is_ok());

        let res3 = fs::remove_file(path);
        assert!(res3.is_ok());
    }
}
