use crate::bloom::BloomFilter;
use crate::storage::{Storage, StorageError};
use crate::zonemap::ZoneMap;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;

const SEP: u8 = b'\t';
const NL: u8 = b'\n';

pub struct SsTable {
    pub path: String,
    pub bloom: BloomFilter,
    pub zone_map: ZoneMap,
}

impl SsTable {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            bloom: BloomFilter::new(1024),
            zone_map: ZoneMap::default(),
        }
    }

    pub async fn create<S: Storage + Sync + Send>(
        path: impl Into<String>,
        entries: &[(String, Vec<u8>)],
        storage: &S,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let mut bloom = BloomFilter::new(1024);
        let mut zone_map = ZoneMap::default();
        let mut data = Vec::new();
        for (k, v) in entries {
            bloom.insert(k);
            zone_map.update(k);
            data.extend_from_slice(k.as_bytes());
            data.push(SEP);
            let enc = STANDARD.encode(v);
            data.extend_from_slice(enc.as_bytes());
            data.push(NL);
        }
        storage.put(&path, data).await?;
        Ok(Self { path, bloom, zone_map })
    }

    pub async fn get<S: Storage + Sync + Send>(
        &self,
        key: &str,
        storage: &S,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if !self.zone_map.contains(key) || !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let raw = storage.get(&self.path).await?;
        for line in raw.split(|b| *b == NL) {
            if line.is_empty() {
                continue;
            }
            if let Some(pos) = line.iter().position(|b| *b == SEP) {
                let (k, v) = line.split_at(pos);
                if k == key.as_bytes() {
                    let val = STANDARD
                        .decode(&v[1..])
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                    return Ok(Some(val));
                }
            }
        }
        Ok(None)
    }
}
