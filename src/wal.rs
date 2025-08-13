use std::{fs::File, io::Write, path::PathBuf};
use tokio::sync::Mutex;

pub struct Wal {
    path: PathBuf,
    file: Mutex<File>,
}

impl Wal {
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = File::create(&path)?;
        Ok(Self { path, file: Mutex::new(file) })
    }

    pub async fn append(&self, data: &[u8]) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.write_all(data)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

pub fn shard_wal(base: &str, shards: usize) -> Vec<Wal> {
    (0..shards)
        .map(|i| Wal::new(format!("{base}_{i}.wal")).expect("create wal"))
        .collect()
}
