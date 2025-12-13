# Notes: Using `fs_usage` to understand storage I/O (macOS)

These notes are written for this repo’s storage experiments (especially `BlockFile` and the `blockfile_workload` benchmark), but the same mental model applies to most DB/storage code on macOS.

## What `fs_usage` is (and is not)

- `fs_usage` is a live tracer for filesystem/disk-related activity on macOS.
- It does **not** run your command for you. If you pass `fs_usage ... my_program args...`, `my_program` is treated as a **process filter string**, not something to execute.
- It usually requires root: run with `sudo`.

Typical workflow is 2 terminals:

1. Terminal A (trace): `sudo fs_usage -w -f filesys -t 10 blockfile_workload`
2. Terminal B (workload): run the benchmark binary.

## The “stack” you are trying to observe

Think in layers:

1. Your code calls `read/write/lseek/sync` (userspace).
2. The kernel may satisfy reads/writes from the **page cache** (RAM) quickly.
3. Later (or when you force it), the kernel issues actual device I/O to storage.

This is why a `write()` syscall can be microseconds while “real durability” takes milliseconds.

## `filesys` mode: what syscalls you’re making

Run:

`sudo fs_usage -w -f filesys -t 10 blockfile_workload`

You’ll see lines like:

`lseek F=3 O=0x20c42000 <SEEK_SET> 0.000001 blockfile_workload.12345`

Field meanings:

- Timestamp (`09:13:59.799895`): when the tracer observed the event.
- Operation (`lseek`, `read`, `write`, `fcntl`, …): syscall/operation name.
- `F=<n>`: file descriptor number in the traced process.
- `O=0x...`: seek offset in bytes (hex is convenient for spotting `0x1000` alignment).
- `<SEEK_SET>`: seek “whence” (`SEEK_SET` is absolute-from-file-start).
- `0.000001`: duration of the operation (seconds).
- `process.pid`: which process emitted the event.

### Mapping to this repo’s `BlockFile`

`BlockFile::write_block()` writes one “page” as:

- `write B=0x4` (4 bytes checksum)
- `write B=0xffc` (4092 bytes payload)

Together: 4096 bytes total per page.

`lseek` offsets should be multiples of the logical page size (`page_size`), because offsets are computed as `block_id * page_size`.

### What `write()` really does

`write()` copies data from your process into the kernel for that file descriptor. In the common buffered I/O path, that usually means:

- the file’s pages in the page cache become **dirty**
- the syscall returns quickly

It does **not** guarantee the data is on disk.

## `FULLFSYNC` / durability: why it’s slow

When you see:

`fcntl F=3 <FULLFSYNC> 0.0049 ...`

That’s the process asking the OS to push prior dirty file data to stable storage (on macOS, this is often what you’ll observe for Rust’s `File::sync_data()`).

In durable-per-write workloads this frequently dominates latency and caps throughput.

Important gotcha:

- `fs_usage` output is a stream and is not always easy to group “per loop iteration”. A `FULLFSYNC` line might belong to the previous operation, depending on timing.

## `diskio` mode: what actually hit the device

Run:

`sudo fs_usage -w -f diskio -t 10 blockfile_workload`

This shows device-level I/O events:

`WrData[A] D=0x015b2c68 B=0x2000 /dev/disk3s5 .../data/block.bin ...`

Field meanings:

- `WrData` / `RdData`: kernel issued a write/read to the device.
- `B=0x...`: size of this I/O (bytes).
- `D=0x...`: device block address (logical block number on the device).
- `/dev/disk...`: which device/volume.
- pathname: which file these pages are associated with (best-effort attribution).

Why you can see multiple `WrData` events per one logical page update:

- the filesystem may need to write **metadata/transaction** updates too
- the kernel may **split/merge** dirty pages into different physical I/Os

So the count of `WrData` is not “number of `write()` syscalls”; it’s closer to “how the filesystem chose to flush to storage”.

## `cachehit` mode: hints about caching (but harder to interpret)

Run:

`sudo fs_usage -w -f cachehit -t 10 blockfile_workload`

This adds `CACHE_HIT A=0x...` lines. These are VM/cache events at an address and are not a direct “block N was cached” signal.

For “was my DB file read from RAM or disk?”, `diskio` is usually clearer:

- Warm-cache read: few/no `RdData` events for your file.
- Cold-ish read: many `RdData` events for your file.

## `pathname` mode: catch unexpected `stat/open` churn

Run:

`sudo fs_usage -w -f pathname -t 10 blockfile_workload`

Useful for diagnosing surprising path activity that can pollute benchmarks (e.g., repeated `stat()` calls, temp files, etc.).

## Practical recipes

### Trace a random-write durable workload

Terminal A:

`sudo fs_usage -w -f filesys -t 10 blockfile_workload`

Terminal B:

`target/release/examples/blockfile_workload data/block.bin write --ops 200000 --pattern rand --sync data`

Look for:

- `lseek` offsets (randomness)
- `write B=0x4` + `write B=0xffc` (one 4KiB page)
- `FULLFSYNC` durations (durability cost)

### Prove whether reads are actually hitting disk

Terminal A:

`sudo fs_usage -w -f diskio -t 10 blockfile_workload | rg 'block\\.bin|RdData|WrData'`

Terminal B:

`target/release/examples/blockfile_workload data/block.bin read --ops 500000 --pattern rand --verify`

If you see many `RdData` lines for `block.bin`, you’re doing real device reads (cold-ish). If not, you’re likely hitting the page cache.

