use lsmt::{storage::local::LocalStorage, Database};

#[tokio::main]
async fn main() {
    let storage = LocalStorage::new("/tmp/lsmt-data");
    let _db = Database::new(storage);
    println!("LSMT server placeholder running");
}
