use super::{Storage, StorageError};
use async_trait::async_trait;

pub struct S3Storage;

impl S3Storage {
    pub fn new(_bucket: &str) -> Self {
        Self
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn put(&self, _path: &str, _data: Vec<u8>) -> Result<(), StorageError> {
        Err(StorageError::Unimplemented)
    }

    async fn get(&self, _path: &str) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::Unimplemented)
    }
}
