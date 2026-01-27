use std::path::PathBuf;
use wrongodb::BTree;

fn main() {
    let db_path: PathBuf = "/tmp/test_recovery.db".into();

    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(db_path.with_extension("db.wal"));

    println!("=== Creating database ===");
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();

        println!("Inserting key0");
        tree.put(b"key0", b"value0").unwrap();

        println!("Syncing WAL");
        tree.sync_wal().unwrap();
    }

    println!("\n=== Reopening database (recovery) ===");
    {
        let mut tree = BTree::open(&db_path, true).unwrap();

        println!("Trying to get key0");
        match tree.get(b"key0") {
            Ok(Some(v)) => println!("Found: {:?}", String::from_utf8_lossy(&v)),
            Ok(None) => println!("Not found (None)"),
            Err(e) => println!("Error: {}", e),
        }
    }
}
