use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError>;
    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError>;
}

#[async_trait]
impl Storage for Box<dyn Storage> {
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        (**self).put(path, data).await
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        (**self).get(path).await
    }
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
