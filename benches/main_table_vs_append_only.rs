use std::fs::OpenOptions;
use std::io::Write;

use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

fn docs() -> Vec<serde_json::Value> {
    (0..2000)
        .map(|i| json!({"name": format!("user-{i}"), "age": i % 100}))
        .collect()
}

fn bench_main_table(c: &mut Criterion) {
    let docs = docs();
    c.bench_function("wrongodb_main_table_insert_2000", |b| {
        b.iter(|| {
            let tmp = tempdir().unwrap();
            let base = tmp.path().join("bench");
            let db = WrongoDB::open(&base).unwrap();
            let coll = db.collection("test");
            let mut session = db.open_session();
            for doc in &docs {
                coll.insert_one(&mut session, doc.clone()).unwrap();
            }
            coll.checkpoint(&mut session).unwrap();
        })
    });
}

fn bench_append_only(c: &mut Criterion) {
    let docs = docs();
    c.bench_function("append_only_log_insert_2000", |b| {
        b.iter(|| {
            let tmp = tempdir().unwrap();
            let append_path = tmp.path().join("append_only.log");
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
        })
    });
}

criterion_group!(benches, bench_main_table, bench_append_only);
criterion_main!(benches);
