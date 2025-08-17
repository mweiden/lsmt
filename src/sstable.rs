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
        let mut sorted = entries.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let mut bloom = BloomFilter::new(1024);
        let mut zone_map = ZoneMap::default();
        let mut data = Vec::new();
        for (k, v) in &sorted {
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

    /// Retrieve a value from the table.
    ///
    /// The bloom filter and zone map are consulted first to avoid unnecessary
    /// I/O. If they indicate the key may exist, the SSTable is read and a
    /// binary search over the sorted entries is performed to locate the key.
    pub async fn get<S: Storage + Sync + Send + ?Sized>(
        &self,
        key: &str,
        storage: &S,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if !self.zone_map.contains(key) || !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let raw = storage.get(&self.path).await?;
        let lines: Vec<&[u8]> = raw
            .split(|b| *b == NL)
            .filter(|line| !line.is_empty())
            .collect();
        if let Some(encoded) = Self::binary_search(&lines, key) {
            let val = STANDARD
                .decode(encoded)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            return Ok(Some(val));
        }
        Ok(None)
    }

    /// Perform a binary search over `lines` to find `key`.
    ///
    /// Each entry in `lines` must be of the form `key\tvalue` and the slice of
    /// lines must be sorted by key in ascending order. When the target key is
    /// found the encoded value slice (the bytes after the separator) is
    /// returned. If the key is not present `None` is returned.
    fn binary_search<'a>(lines: &'a [&'a [u8]], key: &str) -> Option<&'a [u8]> {
        let mut lo = 0;
        let mut hi = lines.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let line = lines[mid];
            if let Some(pos) = line.iter().position(|b| *b == SEP) {
                let key_bytes = &line[..pos];
                match key_bytes.cmp(key.as_bytes()) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => return Some(&line[pos + 1..]),
                }
            } else {
                break;
            }
        }
        None
    }
}
