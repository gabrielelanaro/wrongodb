use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

#[test]
#[ignore]
fn benchmark_main_table_vs_append_only() {
    let tmp = tempdir().unwrap();
    let base = tmp.path().join("bench.db");

    let docs: Vec<_> = (0..2000)
        .map(|i| json!({"name": format!("user-{i}"), "age": i % 100}))
        .collect();

    let main_path = base.clone();
    let start = Instant::now();
    {
        let mut db = WrongoDB::open(&main_path).unwrap();
        {
            let coll = db.collection("test").unwrap();
            for doc in &docs {
                coll.insert_one(doc.clone()).unwrap();
            }
            coll.checkpoint().unwrap();
        }
    }
    let main_elapsed = start.elapsed();

    let append_path = tmp.path().join("append_only.log");
    let start = Instant::now();
    {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&append_path)
            .unwrap();
        for doc in &docs {
            let mut bytes = serde_json::to_vec(doc).unwrap();
            bytes.push(b'\n');
            file.write_all(&bytes).unwrap();
        }
        file.flush().unwrap();
    }
    let append_elapsed = start.elapsed();

    println!("main_table: {:?}", main_elapsed);
    println!("append_only: {:?}", append_elapsed);
}
