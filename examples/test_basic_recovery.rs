use std::path::PathBuf;

use serde_json::json;

use wrongodb::WrongoDB;

fn main() {
    let db_path: PathBuf = "/tmp/test_recovery.db".into();

    let _ = std::fs::remove_dir_all(&db_path);

    println!("=== Creating database ===");
    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        println!("Inserting key0");
        coll.insert_one(&mut session, json!({"_id": "key0", "v": "value0"}))
            .unwrap();
    }

    println!("\n=== Reopening database (recovery) ===");
    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        println!("Trying to get key0");
        let found = coll
            .find_one(&mut session, Some(json!({"_id": "key0"})))
            .unwrap();
        println!("Found: {:?}", found);
    }
}
