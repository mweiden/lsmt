use std::{fs::File, io::Write, path::PathBuf};
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
    /// Create a new log at `path`.
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = File::create(&path)?;
        Ok(Self { path, file: Mutex::new(file) })
    }

    /// Append a line of `data` to the log and flush it to disk.
    pub async fn append(&self, data: &[u8]) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.write_all(data)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

/// Create `shards` independent WAL files using `base` as the prefix.
pub fn shard_wal(base: &str, shards: usize) -> Vec<Wal> {
    (0..shards)
        .map(|i| Wal::new(format!("{base}_{i}.wal")).expect("create wal"))
        .collect()
}
