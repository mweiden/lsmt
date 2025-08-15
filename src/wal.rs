use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use tokio::sync::Mutex;

/// Basic write-ahead log used to persist recent writes before they are
/// flushed to disk.
pub struct Wal {
    /// Path to the log file.
    path: PathBuf,
    /// File handle protected by a mutex for async access.
    file: Mutex<File>,
}

impl Wal {
    /// Create or open a log at `path`.
    ///
    /// The log is opened in append mode so new entries are added to the end.
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Append a line of `data` to the log and flush it to disk.
    pub async fn append(&self, data: &[u8]) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.write_all(data)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }

    /// Remove all data from the log by truncating the underlying file.
    pub async fn clear(&self) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Load all entries currently stored in the log.
    ///
    /// Each line is expected to be of the form `key\tbase64(value)` and will
    /// be decoded into a tuple of `(key, value)`.
    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let mut res = Vec::new();
        let mut buf = Vec::new();
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(res),
            Err(e) => return Err(e),
        };
        file.read_to_end(&mut buf)?;
        for line in buf.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            if let Some(pos) = line.iter().position(|b| *b == b'\t') {
                let key = std::str::from_utf8(&line[..pos])
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                    .to_string();
                let val = STANDARD
                    .decode(&line[pos + 1..])
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                res.push((key, val));
            }
        }
        Ok(res)
    }
}

/// Create `shards` independent WAL files using `base` as the prefix.
pub fn shard_wal(base: &str, shards: usize) -> Vec<Wal> {
    (0..shards)
        .map(|i| Wal::new(format!("{base}_{i}.wal")).expect("create wal"))
        .collect()
}
