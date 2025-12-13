use std::path::PathBuf;
use std::time::Instant;

use wrongodb::BlockFile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Write,
    Read,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pattern {
    Seq,
    Rand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncMode {
    None,
    Data,
    All,
}

#[derive(Debug)]
struct Args {
    path: PathBuf,
    mode: Mode,
    pattern: Pattern,
    create: bool,
    reuse: bool,
    page_size: usize,
    ops: u64,
    range: Option<u64>,
    payload_len: Option<usize>,
    verify: bool,
    sync: SyncMode,
    trace_every: u64,
    trace_limit: u64,
    seed: u64,
}

fn usage() -> &'static str {
    "\
Usage:
  blockfile_workload <PATH> write|read [options]

Options:
  --create              (Deprecated) Same as default behavior for write mode (fresh file).
  --reuse               Reuse existing file for write mode (do not recreate). If missing, write mode recreates the file each run.
  --page-size <N>       Page size (default: 4096; create mode only)
  --ops <N>             Number of operations (default: 100000)
  --range <N>           Data-block id range [1..=N] (default: num data blocks in file, or ops)
  --payload <N>         Payload bytes to write (default: page_size-4)
  --verify              Verify CRC32 on reads
  --pattern seq|rand    Access pattern (default: seq)
  --sync none|data|all  After each write (default: none)
  --seed <N>            RNG seed for rand pattern (default: 1)
  --trace-every <N>     Print every N ops (0 disables; default: 0)
  --trace-limit <N>     Max trace lines (default: 50)
  --help                Show this help

Examples:
  cargo run --release --example blockfile_workload -- data/block.bin write --create --ops 200000 --payload 1024 --pattern seq
  cargo run --release --example blockfile_workload -- data/block.bin write --ops 200000 --pattern rand
  cargo run --release --example blockfile_workload -- data/block.bin write --ops 50000 --pattern rand --sync data
  cargo run --release --example blockfile_workload -- data/block.bin read  --ops 500000 --pattern rand --verify
"
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let Some(path) = it.next() else {
        return Err(usage().into());
    };
    let Some(mode) = it.next() else {
        return Err(usage().into());
    };

    let mode = match mode.as_str() {
        "write" => Mode::Write,
        "read" => Mode::Read,
        "--help" | "-h" => return Err(usage().into()),
        _ => return Err(format!("unknown mode: {mode}\n\n{}", usage())),
    };

    let mut args = Args {
        path: PathBuf::from(path),
        mode,
        pattern: Pattern::Seq,
        create: false,
        reuse: false,
        page_size: 4096,
        ops: 100_000,
        range: None,
        payload_len: None,
        verify: false,
        sync: SyncMode::None,
        trace_every: 0,
        trace_limit: 50,
        seed: 1,
    };

    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--help" | "-h" => return Err(usage().into()),
            "--create" => args.create = true,
            "--reuse" => args.reuse = true,
            "--verify" => args.verify = true,
            "--page-size" => {
                let v = it.next().ok_or("--page-size needs a value")?;
                args.page_size = v.parse::<usize>().map_err(|_| "invalid --page-size")?;
            }
            "--ops" => {
                let v = it.next().ok_or("--ops needs a value")?;
                args.ops = v.parse::<u64>().map_err(|_| "invalid --ops")?;
            }
            "--range" => {
                let v = it.next().ok_or("--range needs a value")?;
                args.range = Some(v.parse::<u64>().map_err(|_| "invalid --range")?);
            }
            "--payload" => {
                let v = it.next().ok_or("--payload needs a value")?;
                args.payload_len = Some(v.parse::<usize>().map_err(|_| "invalid --payload")?);
            }
            "--pattern" => {
                let v = it.next().ok_or("--pattern needs a value")?;
                args.pattern = match v.as_str() {
                    "seq" => Pattern::Seq,
                    "rand" => Pattern::Rand,
                    _ => return Err("invalid --pattern (use seq|rand)".into()),
                };
            }
            "--sync" => {
                let v = it.next().ok_or("--sync needs a value")?;
                args.sync = match v.as_str() {
                    "none" => SyncMode::None,
                    "data" => SyncMode::Data,
                    "all" => SyncMode::All,
                    _ => return Err("invalid --sync (use none|data|all)".into()),
                };
            }
            "--seed" => {
                let v = it.next().ok_or("--seed needs a value")?;
                args.seed = v.parse::<u64>().map_err(|_| "invalid --seed")?;
            }
            "--trace-every" => {
                let v = it.next().ok_or("--trace-every needs a value")?;
                args.trace_every = v.parse::<u64>().map_err(|_| "invalid --trace-every")?;
            }
            "--trace-limit" => {
                let v = it.next().ok_or("--trace-limit needs a value")?;
                args.trace_limit = v.parse::<u64>().map_err(|_| "invalid --trace-limit")?;
            }
            _ => return Err(format!("unknown flag: {flag}\n\n{}", usage())),
        }
    }

    if args.create && args.mode != Mode::Write {
        return Err("--create only makes sense with write mode".into());
    }
    if args.reuse && args.mode != Mode::Write {
        return Err("--reuse only makes sense with write mode".into());
    }
    if args.create && args.reuse {
        return Err("use at most one of --create and --reuse".into());
    }
    if args.sync != SyncMode::None && args.mode != Mode::Write {
        return Err("--sync only makes sense with write mode".into());
    }

    Ok(args)
}

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn main() {
    let args = match parse_args() {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    // Benchmark default: write mode starts from a fresh file each run.
    // Use --reuse to keep and update an existing file.
    let should_create = match args.mode {
        Mode::Write => !args.reuse,
        Mode::Read => false,
    };

    if args.mode == Mode::Write && should_create && args.path.exists() {
        if let Err(e) = std::fs::remove_file(&args.path) {
            eprintln!("failed to remove existing file {:?}: {e}", args.path);
            std::process::exit(2);
        }
    }

    let mut bf = if should_create {
        match BlockFile::create(&args.path, args.page_size) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("failed to create block file at {:?}: {e}", args.path);
                std::process::exit(2);
            }
        }
    } else {
        match BlockFile::open(&args.path) {
            Ok(v) => v,
            Err(e) => {
                match args.mode {
                    Mode::Write => {
                        eprintln!(
                            "failed to open existing file {:?} as a BlockFile: {e}\n\
Hint: omit `--reuse` (default behavior) to recreate it as a block file format.",
                            args.path
                        );
                        std::process::exit(2);
                    }
                    Mode::Read => {
                        eprintln!("failed to open block file at {:?}: {e}", args.path);
                        std::process::exit(2);
                    }
                }
            }
        }
    };

    let inferred_range = match args.mode {
        Mode::Write => args.ops,
        Mode::Read => bf.num_blocks().unwrap_or(0).saturating_sub(1),
    };
    let range = args.range.unwrap_or(inferred_range).max(1);

    if args.mode == Mode::Read && inferred_range == 0 {
        eprintln!(
            "block file {:?} has no data blocks (only header); run a write workload first",
            args.path
        );
        std::process::exit(2);
    }

    if args.mode == Mode::Write {
        let target_blocks = range.saturating_add(1);
        loop {
            let current = bf.num_blocks().unwrap_or(0);
            if current >= target_blocks {
                break;
            }
            let id = bf.allocate_block().unwrap();
            // Initialize with a valid checksum so subsequent reads with --verify work even if
            // a given block id isn't written during this run.
            bf.write_block(id, &[]).unwrap();
        }
    }

    let max_payload = bf.page_size.saturating_sub(4);
    let payload_len = args.payload_len.unwrap_or(max_payload).min(max_payload);
    let mut payload = vec![0u8; payload_len];

    let start = Instant::now();
    let mut rng_state = args.seed;
    let mut traced = 0u64;

    for i in 0..args.ops {
        let block_id = 1
            + match args.pattern {
                Pattern::Seq => i % range,
                Pattern::Rand => xorshift64(&mut rng_state) % range,
            };
        let offset = block_id.saturating_mul(bf.page_size as u64);

        if args.trace_every != 0 && (i % args.trace_every == 0) && traced < args.trace_limit {
            eprintln!(
                "op={i} mode={:?} block_id={block_id} offset={offset} payload_len={payload_len}",
                args.mode
            );
            traced += 1;
        }

        match args.mode {
            Mode::Write => {
                // Make data dependent on block_id to avoid trivial “all zeros” optimizations in
                // downstream layers (compression, sparse writes, etc).
                let fill = (block_id as u8).wrapping_mul(31).wrapping_add(17);
                payload.fill(fill);
                bf.write_block(block_id, &payload).unwrap();
                match args.sync {
                    SyncMode::None => {}
                    SyncMode::Data => bf.sync_data().unwrap(),
                    SyncMode::All => bf.sync_all().unwrap(),
                }
            }
            Mode::Read => {
                let _ = bf.read_block(block_id, args.verify).unwrap();
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    let ops_s = (args.ops as f64) / elapsed;
    let bytes_total = (args.ops as f64) * (payload_len as f64);
    let mib_s = (bytes_total / (1024.0 * 1024.0)) / elapsed;

    println!(
        "mode={:?} pattern={:?} ops={} elapsed={:.3}s ops/s={:.0} MiB/s={:.1} page_size={} payload_len={} range={} verify={} sync={:?}",
        args.mode,
        args.pattern,
        args.ops,
        elapsed,
        ops_s,
        mib_s,
        bf.page_size,
        payload_len,
        range,
        args.verify,
        args.sync
    );
}
