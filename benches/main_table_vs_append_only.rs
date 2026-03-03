use std::fs::OpenOptions;
use std::io::Write;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig};

fn docs() -> Vec<(String, String)> {
    (0..2000)
        .map(|i| (format!("user-{i}"), format!("age:{}", i % 100)))
        .collect()
}

fn bench_main_table(c: &mut Criterion) {
    let docs = docs();
    c.bench_function("wrongodb_main_table_insert_2000", |b| {
        b.iter(|| {
            let tmp = tempdir().unwrap();
            let base = tmp.path().join("bench");
            let conn = Connection::open(&base, ConnectionConfig::default()).unwrap();

            for (k, v) in &docs {
                let mut session = conn.open_session();
                let mut txn = session.transaction().unwrap();
                let txn_id = txn.as_ref().id();
                let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();
                cursor.insert(k.as_bytes(), v.as_bytes(), txn_id).unwrap();
                txn.commit().unwrap();
            }
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
            for (k, v) in &docs {
                let line = format!("{k}:{v}\n");
                file.write_all(line.as_bytes()).unwrap();
            }
            file.flush().unwrap();
        })
    });
}

criterion_group!(benches, bench_main_table, bench_append_only);
criterion_main!(benches);
