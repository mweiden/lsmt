pub struct BloomFilter {
    bits: Vec<bool>,
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        Self { bits: vec![false; size] }
    }

    pub fn insert(&mut self, _item: &str) {
        // TODO: hash and set bits
    }

    pub fn may_contain(&self, _item: &str) -> bool {
        // TODO: proper check
        true
    }
}
