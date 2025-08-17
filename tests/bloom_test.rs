use cass::bloom::BloomFilter;

#[test]
fn bloom_insert_check() {
    let mut b = BloomFilter::new(128);
    b.insert("hello");
    assert!(b.may_contain("hello"));
}
