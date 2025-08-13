use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError>;
    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError>;
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unimplemented")]
    Unimplemented,
}

pub mod local;
pub mod s3;
