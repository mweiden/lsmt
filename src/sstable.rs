use crate::bloom::BloomFilter;
use crate::zonemap::ZoneMap;

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
}
