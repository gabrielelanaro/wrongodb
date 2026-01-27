use tempfile::tempdir;
use wrongodb::BTree;

fn main() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    
    // Phase 1: Create and insert initial data
    println!("=== Phase 1: Create and insert 0-4 ===");
    {
        let mut tree = BTree::create(&db_path, 512, true).unwrap();
        
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            println!("Inserting: {}", key);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        println!("Checkpointing...");
        tree.checkpoint().unwrap();
        println!("Checkpoint done");
    }
    
    // Phase 2: Open and insert more data
    println!("\n=== Phase 2: Open and insert 5-9 ===");
    {
        let mut tree = BTree::open(&db_path, true).unwrap();
        
        for i in 5..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            println!("Inserting: {}", key);
            tree.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Don't checkpoint - simulate crash
    }
    
    // Phase 3: Reopen and check
    println!("\n=== Phase 3: Reopen and check ===");
    {
        let mut tree = BTree::open(&db_path, true).unwrap();
        
        for i in 0..10 {
            let key = format!("key{}", i);
            let result = tree.get(key.as_bytes()).unwrap();
            match &result {
                Some(v) => println!("{} -> {:?}", key, String::from_utf8_lossy(v)),
                None => println!("{} -> MISSING", key),
            }
        }
    }
}
