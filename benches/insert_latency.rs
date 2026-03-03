use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use wrongodb::{Connection, ConnectionConfig};

const VALUE_SIZE: usize = 100;
const DB_SIZES: &[usize] = &[0, 1_000, 10_000, 100_000];

static KEY_COUNTER: AtomicU64 = AtomicU64::new(1_000_000);

fn bench_data_dir() -> PathBuf {
    PathBuf::from("target/bench-data-latency")
}

fn cleanup() {
    let _ = fs::remove_dir_all(bench_data_dir());
}

fn db_path(name: &str) -> PathBuf {
    bench_data_dir().join(name)
}

fn generate_data(size: usize) -> String {
    "x".repeat(size)
}

fn create_db(name: &str) -> Connection {
    let path = db_path(name);
    let _ = fs::remove_dir_all(&path);
    Connection::open(&path, ConnectionConfig::default()).expect("Failed to create database")
}

fn sequential_key(i: usize) -> String {
    format!("key_{:010}", i)
}

fn insert_kv(conn: &Connection, key: &str, value: &str) {
    let mut session = conn.open_session();
    let mut txn = session.transaction().expect("begin tx failed");
    let txn_id = txn.as_ref().id();
    let mut cursor = txn
        .session_mut()
        .open_cursor("table:test")
        .expect("open cursor failed");
    cursor
        .insert(key.as_bytes(), value.as_bytes(), txn_id)
        .expect("insert failed");
    txn.commit().expect("commit failed");
}

fn pre_populate(conn: &Connection, count: usize) {
    let data = generate_data(VALUE_SIZE);
    for i in 0..count {
        let key = sequential_key(i);
        insert_kv(conn, &key, &data);
    }
}

fn insert_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_latency");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(30));

    let data = generate_data(VALUE_SIZE);

    cleanup();

    for db_size in DB_SIZES {
        let db_name = format!("bench_{}_entries", db_size);

        let conn = create_db(&db_name);
        pre_populate(&conn, *db_size);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_entries", db_size)),
            db_size,
            |b, _| {
                b.iter(|| {
                    let key = format!(
                        "bench_key_{:016x}",
                        KEY_COUNTER.fetch_add(1, Ordering::SeqCst)
                    );
                    insert_kv(&conn, &key, &data);
                    black_box(&conn);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, insert_latency);
criterion_main!(benches);
