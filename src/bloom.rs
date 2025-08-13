pub struct BloomFilter {
    bits: Vec<bool>,
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        Self { bits: vec![false; size] }
    }

    fn hashes(&self, item: &str) -> (usize, usize) {
        let mut h1: u64 = 5381;
        let mut h2: u64 = 0;
        for b in item.bytes() {
            h1 = ((h1 << 5).wrapping_add(h1)).wrapping_add(b as u64);
            h2 = h2.wrapping_mul(31).wrapping_add(b as u64);
        }
        let len = self.bits.len() as u64;
        ((h1 % len) as usize, (h2 % len) as usize)
    }

    pub fn insert(&mut self, item: &str) {
        let (a, b) = self.hashes(item);
        self.bits[a] = true;
        self.bits[b] = true;
    }

    pub fn may_contain(&self, item: &str) -> bool {
        let (a, b) = self.hashes(item);
        self.bits.get(a).copied().unwrap_or(false)
            && self.bits.get(b).copied().unwrap_or(false)
    }
}
