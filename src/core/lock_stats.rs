use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub enum LockStatKind {
    Table,
    Wal,
    MvccShard,
    Checkpoint,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LockCounterSnapshot {
    pub acquires: u64,
    pub wait_ns: u64,
    pub hold_ns: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LockStatsSnapshot {
    pub table: LockCounterSnapshot,
    pub wal: LockCounterSnapshot,
    pub mvcc_shard: LockCounterSnapshot,
    pub checkpoint: LockCounterSnapshot,
}

#[derive(Debug, Default)]
struct LockCounter {
    acquires: AtomicU64,
    wait_ns: AtomicU64,
    hold_ns: AtomicU64,
}

impl LockCounter {
    fn record_wait(&self, wait: Duration) {
        self.acquires.fetch_add(1, Ordering::Relaxed);
        self.wait_ns
            .fetch_add(duration_as_u64_ns(wait), Ordering::Relaxed);
    }

    fn record_hold(&self, hold: Duration) {
        self.hold_ns
            .fetch_add(duration_as_u64_ns(hold), Ordering::Relaxed);
    }

    fn snapshot(&self) -> LockCounterSnapshot {
        LockCounterSnapshot {
            acquires: self.acquires.load(Ordering::Relaxed),
            wait_ns: self.wait_ns.load(Ordering::Relaxed),
            hold_ns: self.hold_ns.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.acquires.store(0, Ordering::Relaxed);
        self.wait_ns.store(0, Ordering::Relaxed);
        self.hold_ns.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct LockStats {
    enabled: AtomicBool,
    table: LockCounter,
    wal: LockCounter,
    mvcc_shard: LockCounter,
    checkpoint: LockCounter,
}

impl LockStats {
    fn counter(&self, kind: LockStatKind) -> &LockCounter {
        match kind {
            LockStatKind::Table => &self.table,
            LockStatKind::Wal => &self.wal,
            LockStatKind::MvccShard => &self.mvcc_shard,
            LockStatKind::Checkpoint => &self.checkpoint,
        }
    }
}

static GLOBAL_LOCK_STATS: OnceLock<LockStats> = OnceLock::new();

fn global_lock_stats() -> &'static LockStats {
    GLOBAL_LOCK_STATS.get_or_init(LockStats::default)
}

pub fn set_lock_stats_enabled(enabled: bool) {
    global_lock_stats()
        .enabled
        .store(enabled, Ordering::Relaxed);
}

pub fn lock_stats_enabled() -> bool {
    global_lock_stats().enabled.load(Ordering::Relaxed)
}

pub fn reset_lock_stats() {
    let stats = global_lock_stats();
    stats.table.reset();
    stats.wal.reset();
    stats.mvcc_shard.reset();
    stats.checkpoint.reset();
}

pub fn snapshot_lock_stats() -> LockStatsSnapshot {
    let stats = global_lock_stats();
    LockStatsSnapshot {
        table: stats.table.snapshot(),
        wal: stats.wal.snapshot(),
        mvcc_shard: stats.mvcc_shard.snapshot(),
        checkpoint: stats.checkpoint.snapshot(),
    }
}

pub fn record_lock_wait(kind: LockStatKind, wait: Duration) {
    if !lock_stats_enabled() {
        return;
    }
    global_lock_stats().counter(kind).record_wait(wait);
}

pub fn begin_lock_hold(kind: LockStatKind) -> LockHoldGuard {
    LockHoldGuard {
        kind,
        start: Instant::now(),
        enabled: lock_stats_enabled(),
    }
}

pub struct LockHoldGuard {
    kind: LockStatKind,
    start: Instant,
    enabled: bool,
}

impl Drop for LockHoldGuard {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        global_lock_stats()
            .counter(self.kind)
            .record_hold(self.start.elapsed());
    }
}

fn duration_as_u64_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}
