use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use wrongodb::WrongoDB;

const VALUE_SIZE: usize = 100;

/// Database sizes to test (number of pre-existing entries)
const DB_SIZES: &[usize] = &[0, 1_000, 10_000, 100_000];

/// Starting counter for unique key generation (uses high bits to avoid collision with pre-populated keys)
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

fn create_db(name: &str) -> WrongoDB {
    let path = db_path(name);
    // Clean up any existing database first
    let _ = fs::remove_dir_all(&path);
    // Open with no secondary indexes, sync_every_write: false for performance
    WrongoDB::open(&path).expect("Failed to create database")
}

fn sequential_key(i: usize) -> String {
    format!("key_{:010}", i)
}

fn pre_populate(db: &mut WrongoDB, count: usize) {
    let data = generate_data(VALUE_SIZE);
    let coll = db.collection("test").expect("Failed to open collection");
    for i in 0..count {
        let key = sequential_key(i);
        let doc = json!({
            "_id": key,
            "data": &data
        });
        coll.insert_one(doc).expect("Failed to insert");
    }
}

fn insert_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_latency");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(30));

    let data = generate_data(VALUE_SIZE);

    // Clean up any stale benchmark data first
    cleanup();

    for db_size in DB_SIZES {
        let db_name = format!("bench_{}_entries", db_size);

        // Setup: Create database and pre-populate
        let mut db = create_db(&db_name);
        pre_populate(&mut db, *db_size);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_entries", db_size)),
            db_size,
            |b, _| {
                b.iter(|| {
                    // Generate a unique key for each iteration to avoid duplicate key errors
                    let key = format!(
                        "bench_key_{:016x}",
                        KEY_COUNTER.fetch_add(1, Ordering::SeqCst)
                    );
                    let doc = json!({
                        "_id": key,
                        "data": &data
                    });
                    {
                        let coll = db.collection("test").expect("Failed to open collection");
                        coll.insert_one(doc).expect("Insert failed");
                    }
                    black_box(&db);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, insert_latency);
criterion_main!(benches);
