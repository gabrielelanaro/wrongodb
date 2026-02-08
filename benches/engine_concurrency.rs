use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Barrier;
use std::thread;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use wrongodb::{
    reset_lock_stats, set_lock_stats_enabled, snapshot_lock_stats, WrongoDB, WrongoDBConfig,
};

const CONCURRENCY_LEVELS: &[usize] = &[1, 4, 8, 16];
const PAYLOAD_SIZE: usize = 1024;
// Keep per-iteration work large enough that benchmark results are dominated by DB work,
// not thread spawn/join overhead from the harness.
const INSERTS_PER_WORKER: usize = 512;
const UPDATES_PER_WORKER: usize = 4096;

static NEXT_DOC_ID: AtomicU64 = AtomicU64::new(0);
static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);

fn bench_data_dir() -> PathBuf {
    PathBuf::from("target/bench-data-engine-concurrency")
}

fn open_bench_db(label: &str) -> WrongoDB {
    let db_id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
    let path = bench_data_dir().join(format!("{label}-{db_id}"));
    let _ = fs::remove_dir_all(&path);
    WrongoDB::open_with_config(
        &path,
        WrongoDBConfig::new()
            .wal_enabled(true)
            .lock_stats_enabled(true),
    )
    .expect("failed to open benchmark database")
}

fn write_lock_stats(label: &str) {
    let path = bench_data_dir().join(format!("lock-stats-{label}.json"));
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let snapshot = snapshot_lock_stats();
    if let Ok(bytes) = serde_json::to_vec_pretty(&snapshot) {
        let _ = fs::write(path, bytes);
    }
}

fn run_insert_unique_batch(
    db: &WrongoDB,
    collection_name: &str,
    concurrency: usize,
    ops_per_worker: usize,
    payload: &str,
) {
    let barrier = Barrier::new(concurrency);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(concurrency);
        for worker_id in 0..concurrency {
            let barrier_ref = &barrier;
            handles.push(scope.spawn(move || {
                let coll = db.collection(collection_name);
                let mut session = db.open_session();
                barrier_ref.wait();

                for _ in 0..ops_per_worker {
                    let doc_id = NEXT_DOC_ID.fetch_add(1, Ordering::Relaxed);
                    let doc = json!({
                        "_id": format!("w{worker_id}-{doc_id}"),
                        "payload": payload,
                        "k": doc_id as i64,
                    });
                    coll.insert_one(&mut session, doc)
                        .expect("insert_unique benchmark insert failed");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("insert_unique worker panicked");
        }
    });
}

fn run_update_hotspot_batch(
    db: &WrongoDB,
    collection_name: &str,
    concurrency: usize,
    ops_per_worker: usize,
) {
    let barrier = Barrier::new(concurrency);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let barrier_ref = &barrier;
            handles.push(scope.spawn(move || {
                let coll = db.collection(collection_name);
                let mut session = db.open_session();
                let filter = Some(json!({ "_id": "hot" }));
                let update = json!({ "$inc": { "k": 1_i64 } });
                barrier_ref.wait();

                for _ in 0..ops_per_worker {
                    coll.update_one(&mut session, filter.clone(), update.clone())
                        .expect("update_hotspot benchmark update failed");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("update_hotspot worker panicked");
        }
    });
}

fn bench_engine_insert_unique_scaling(c: &mut Criterion) {
    let _ = fs::remove_dir_all(bench_data_dir());
    set_lock_stats_enabled(true);
    reset_lock_stats();
    let payload = "x".repeat(PAYLOAD_SIZE);

    let mut group = c.benchmark_group("engine_insert_unique_scaling");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(6));

    for &concurrency in CONCURRENCY_LEVELS {
        let db = open_bench_db("insert_unique");
        let coll_name = format!("insert_unique_c{concurrency}");
        group.throughput(Throughput::Elements(
            (concurrency * INSERTS_PER_WORKER) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("c{concurrency}")),
            &concurrency,
            |b, _| {
                b.iter(|| {
                    run_insert_unique_batch(
                        &db,
                        &coll_name,
                        concurrency,
                        INSERTS_PER_WORKER,
                        &payload,
                    )
                });
            },
        );
    }

    group.finish();
    write_lock_stats("engine_insert_unique_scaling");
}

fn bench_engine_update_hotspot_scaling(c: &mut Criterion) {
    let _ = fs::remove_dir_all(bench_data_dir());
    set_lock_stats_enabled(true);
    reset_lock_stats();

    let mut group = c.benchmark_group("engine_update_hotspot_scaling");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(6));

    for &concurrency in CONCURRENCY_LEVELS {
        let db = open_bench_db("update_hotspot");
        let coll_name = format!("update_hotspot_c{concurrency}");
        let coll = db.collection(&coll_name);
        let mut session = db.open_session();
        coll.insert_one(
            &mut session,
            json!({
                "_id": "hot",
                "k": 0_i64,
                "payload": "hot",
            }),
        )
        .expect("failed to seed hotspot document");

        group.throughput(Throughput::Elements(
            (concurrency * UPDATES_PER_WORKER) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("c{concurrency}")),
            &concurrency,
            |b, _| {
                b.iter(|| {
                    run_update_hotspot_batch(&db, &coll_name, concurrency, UPDATES_PER_WORKER)
                });
            },
        );
    }

    group.finish();
    write_lock_stats("engine_update_hotspot_scaling");
}

criterion_group!(
    benches,
    bench_engine_insert_unique_scaling,
    bench_engine_update_hotspot_scaling
);
criterion_main!(benches);
