/// Simple bloom filter implementation used to provide probabilistic
/// membership tests for on-disk SSTables.
pub struct BloomFilter {
    /// Bit vector storing hashed membership information.
    bits: Vec<bool>,
}

impl BloomFilter {
    /// Create a new filter with `size` bits.
    pub fn new(size: usize) -> Self {
        Self {
            bits: vec![false; size],
        }
    }

    /// Compute two simple hash values for `item` using a pair of
    /// different algorithms. These are inexpensive and good enough for
    /// the toy implementation.
    fn hashes(&self, item: &str) -> (usize, usize) {
        // djb2 style hash
        let mut h1: u64 = 5381;
        // second hash accumulates bytes with a different multiplier
        let mut h2: u64 = 0;
        for b in item.bytes() {
            h1 = ((h1 << 5).wrapping_add(h1)).wrapping_add(b as u64);
            h2 = h2.wrapping_mul(31).wrapping_add(b as u64);
        }
        let len = self.bits.len() as u64;
        ((h1 % len) as usize, (h2 % len) as usize)
    }

    /// Record `item` in the filter.
    pub fn insert(&mut self, item: &str) {
        let (a, b) = self.hashes(item);
        self.bits[a] = true;
        self.bits[b] = true;
    }

    /// Return `true` if the filter indicates that `item` may be present.
    /// False positives are possible but false negatives are not.
    pub fn may_contain(&self, item: &str) -> bool {
        let (a, b) = self.hashes(item);
        self.bits.get(a).copied().unwrap_or(false) && self.bits.get(b).copied().unwrap_or(false)
    }

    /// Serialize the bloom filter into a compact byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.bits.iter().map(|b| *b as u8).collect()
    }

    /// Reconstruct a bloom filter from a byte slice produced by
    /// [`to_bytes`].
    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            bits: data.iter().map(|b| *b != 0).collect(),
        }
    }
}
