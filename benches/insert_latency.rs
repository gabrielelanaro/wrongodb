use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use wrongodb::BTree;

const PAGE_SIZE: usize = 4096;
const WAL_ENABLED: bool = true;
const VALUE_SIZE: usize = 100;
const SEED: u64 = 42;

/// Database sizes to test (number of pre-existing entries)
const DB_SIZES: &[usize] = &[0, 1_000, 10_000, 100_000];

/// Number of inserts to measure for each database size
const SAMPLE_SIZE: usize = 1000;

fn bench_data_dir() -> PathBuf {
    PathBuf::from("target/bench-data-latency")
}

fn cleanup() {
    let _ = fs::remove_dir_all(bench_data_dir());
}

fn db_path(name: &str) -> PathBuf {
    bench_data_dir().join(name)
}

fn generate_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn create_btree(name: &str) -> BTree {
    let path = db_path(name);
    // Clean up any existing database first
    let _ = fs::remove_dir_all(&path);
    BTree::create(&path, PAGE_SIZE, WAL_ENABLED).expect("Failed to create B-tree")
}

fn sequential_key(i: usize) -> Vec<u8> {
    format!("key_{:010}", i).into_bytes()
}

fn random_key(rng: &mut StdRng) -> Vec<u8> {
    let val = rng.next_u64();
    format!("key_{:016x}", val).into_bytes()
}

fn pre_populate(btree: &mut BTree, count: usize) {
    let value = generate_value(VALUE_SIZE);
    for i in 0..count {
        let key = sequential_key(i);
        btree.put(&key, &value).expect("Failed to insert");
    }
}

fn insert_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_latency");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(30));

    let value = generate_value(VALUE_SIZE);

    // Clean up any stale benchmark data first
    cleanup();

    for db_size in DB_SIZES {
        let db_name = format!("bench_{}_entries", db_size);

        // Setup: Create database and pre-populate
        let mut btree = create_btree(&db_name);
        pre_populate(&mut btree, *db_size);

        // Generate random keys that don't exist in the database
        let mut rng = StdRng::seed_from_u64(SEED);
        let keys: Vec<Vec<u8>> = (0..SAMPLE_SIZE)
            .map(|_| random_key(&mut rng))
            .collect();

        let key_index = std::cell::Cell::new(0);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_entries", db_size)),
            db_size,
            |b, _| {
                b.iter(|| {
                    let idx = key_index.get();
                    let key = &keys[idx % keys.len()];
                    btree.put(key, &value).expect("Insert failed");
                    key_index.set(idx + 1);
                    black_box(&btree);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, insert_latency);
criterion_main!(benches);
