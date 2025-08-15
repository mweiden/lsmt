use crate::bloom::BloomFilter;
use crate::storage::{Storage, StorageError};
use crate::zonemap::ZoneMap;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

// field delimiters within on-disk table files
const SEP: u8 = b'\t';
const NL: u8 = b'\n';

/// Simplified on-disk sorted string table used for persisting flushed
/// memtable data.
pub struct SsTable {
    /// Path to the file storing the SSTable contents.
    pub path: String,
    /// Bloom filter over the keys to quickly rule out non-existent lookups.
    pub bloom: BloomFilter,
    /// Zone map storing min/max keys for coarse filtering.
    pub zone_map: ZoneMap,
}

impl SsTable {
    /// Create an empty [`SsTable`] with default bloom filter and zone map.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            bloom: BloomFilter::new(1024),
            zone_map: ZoneMap::default(),
        }
    }

    /// Write a new table to disk from the provided `entries` and return
    /// the constructed [`SsTable`] metadata.
    pub async fn create<S: Storage + Sync + Send + ?Sized>(
        path: impl Into<String>,
        entries: &[(String, Vec<u8>)],
        storage: &S,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let mut bloom = BloomFilter::new(1024);
        let mut zone_map = ZoneMap::default();
        let mut data = Vec::new();
        for (k, v) in entries {
            // update auxiliary structures
            bloom.insert(k);
            zone_map.update(k);
            // write "key\tbase64(value)\n" lines
            data.extend_from_slice(k.as_bytes());
            data.push(SEP);
            let enc = STANDARD.encode(v);
            data.extend_from_slice(enc.as_bytes());
            data.push(NL);
        }
        storage.put(&path, data).await?;
        Ok(Self {
            path,
            bloom,
            zone_map,
        })
    }

    /// Retrieve a value from the table, consulting bloom filter and zone
    /// map before scanning the file.
    pub async fn get<S: Storage + Sync + Send + ?Sized>(
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
            // locate separator between key and value
            if let Some(pos) = line.iter().position(|b| *b == SEP) {
                let (k, v) = line.split_at(pos);
                if k == key.as_bytes() {
                    // value bytes are base64 encoded after the separator
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
