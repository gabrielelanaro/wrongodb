use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bson::{doc, Document};
use mongodb::{options::ClientOptions, Client, Collection};
use serde::Serialize;
use tokio::sync::Barrier;

const DB_NAME: &str = "bench";
const DEFAULT_CONCURRENCY: &str = "1,4,8,16,32,64";
const DEFAULT_MONGO_IMAGE: &str = "mongo:7.0";
const DEFAULT_WARMUP_SECS: u64 = 15;
const DEFAULT_MEASURE_SECS: u64 = 60;
const DEFAULT_REPETITIONS: usize = 3;
const READY_TIMEOUT_SECS: u64 = 180;
const STDERR_TAIL_LINES: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Scenario {
    InsertUnique,
    UpdateHotspot,
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::InsertUnique => "insert_unique",
            Self::UpdateHotspot => "update_hotspot",
        }
    }

    fn parse(value: &str) -> Result<Vec<Self>, String> {
        match value {
            "insert_unique" => Ok(vec![Self::InsertUnique]),
            "update_hotspot" => Ok(vec![Self::UpdateHotspot]),
            "all" => Ok(vec![Self::InsertUnique, Self::UpdateHotspot]),
            other => Err(format!(
                "invalid --scenario value '{other}' (expected insert_unique, update_hotspot, or all)"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    warmup_secs: u64,
    measure_secs: u64,
    concurrencies: Vec<usize>,
    mongo_image: String,
    out_dir: PathBuf,
    scenarios: Vec<Scenario>,
    repetitions: usize,
}

#[derive(Debug, Clone)]
struct RunResult {
    run_id: String,
    backend: String,
    scenario: Scenario,
    concurrency: usize,
    ops_total: u64,
    ops_per_sec: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    error_rate: f64,
}

#[derive(Debug, Default)]
struct WorkerMetrics {
    ops_ok: u64,
    ops_err: u64,
    latencies_us: Vec<u32>,
}

#[derive(Debug, Serialize)]
struct GateReport {
    classification: String,
    wrongo_scale: f64,
    mongo_scale: f64,
    scale_gap: f64,
    notes: String,
}

#[derive(Debug)]
struct WrongoBackend {
    addr: String,
    uri: String,
    db_path: PathBuf,
    child: Option<Child>,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
}

#[derive(Debug)]
struct MongoBackend {
    image: String,
    port: Option<u16>,
    container_name: Option<String>,
    uri: Option<String>,
}

#[derive(Debug)]
enum BackendKind {
    Wrongo(WrongoBackend),
    Mongo(MongoBackend),
}

impl BackendKind {
    fn name(&self) -> &'static str {
        match self {
            Self::Wrongo(_) => "wrongodb",
            Self::Mongo(_) => "mongodb",
        }
    }

    async fn start(&mut self) -> Result<(), String> {
        match self {
            Self::Wrongo(backend) => backend.start().await,
            Self::Mongo(backend) => backend.start().await,
        }
    }

    fn ready_uri(&self) -> Result<&str, String> {
        match self {
            Self::Wrongo(backend) => Ok(&backend.uri),
            Self::Mongo(backend) => backend
                .uri
                .as_deref()
                .ok_or_else(|| "mongo backend not started".to_string()),
        }
    }

    async fn prepare_dataset(
        &self,
        collection: &Collection<Document>,
        scenario: Scenario,
    ) -> Result<(), String> {
        match scenario {
            Scenario::InsertUnique => Ok(()),
            Scenario::UpdateHotspot => {
                collection
                    .insert_one(doc! {
                        "_id": "hot",
                        "k": 0_i64,
                        "payload": "hot",
                    })
                    .await
                    .map_err(|e| format!("failed to prepare hotspot doc: {e}"))?;
                Ok(())
            }
        }
    }

    fn cleanup(&mut self) -> Result<(), String> {
        match self {
            Self::Wrongo(backend) => backend.cleanup(),
            Self::Mongo(backend) => backend.cleanup(),
        }
    }
}

impl WrongoBackend {
    fn new(out_dir: &Path, port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}");
        let uri = format!("mongodb://{addr}");
        let db_path = out_dir.join("tmp").join("wrongodb-data");
        Self {
            addr,
            uri,
            db_path,
            child: None,
            stderr_tail: Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES))),
        }
    }

    async fn start(&mut self) -> Result<(), String> {
        if self.db_path.exists() {
            fs::remove_dir_all(&self.db_path).map_err(|e| {
                format!(
                    "failed to reset WrongoDB bench path '{}': {e}",
                    self.db_path.display()
                )
            })?;
        }
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "failed to create WrongoDB bench parent path '{}': {e}",
                    parent.display()
                )
            })?;
        }

        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--release")
            .arg("--bin")
            .arg("wrongodb-server")
            .arg("--")
            .arg("--addr")
            .arg(&self.addr)
            .arg("--db-path")
            .arg(&self.db_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to start wrongodb-server: {e}"))?;

        if let Some(stderr) = child.stderr.take() {
            let tail = Arc::clone(&self.stderr_tail);
            std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    if let Ok(mut guard) = tail.lock() {
                        if guard.len() == STDERR_TAIL_LINES {
                            guard.pop_front();
                        }
                        guard.push_back(line);
                    }
                }
            });
        }

        self.child = Some(child);

        if let Err(err) = wait_for_ready(&self.uri, Duration::from_secs(READY_TIMEOUT_SECS)).await {
            let _ = self.cleanup();
            return Err(format!(
                "WrongoDB failed readiness probe at {}: {err}\nrecent stderr:\n{}",
                self.uri,
                self.stderr_tail()
            ));
        }

        Ok(())
    }

    fn stderr_tail(&self) -> String {
        if let Ok(guard) = self.stderr_tail.lock() {
            if guard.is_empty() {
                return "(no stderr output)".to_string();
            }
            return guard.iter().cloned().collect::<Vec<_>>().join("\n");
        }
        "(stderr unavailable)".to_string()
    }

    fn cleanup(&mut self) -> Result<(), String> {
        if let Some(mut child) = self.child.take() {
            if let Err(e) = child.kill() {
                if !matches!(e.kind(), std::io::ErrorKind::InvalidInput) {
                    return Err(format!("failed to stop wrongodb-server: {e}"));
                }
            }
            let _ = child.wait();
        }
        Ok(())
    }
}

impl MongoBackend {
    fn new(image: String) -> Self {
        Self {
            image,
            port: None,
            container_name: None,
            uri: None,
        }
    }

    async fn start(&mut self) -> Result<(), String> {
        ensure_docker_available()?;

        let mut last_err: Option<String> = None;

        for _ in 0..5 {
            let port = find_free_port().map_err(|e| format!("failed to find free port: {e}"))?;
            let name = format!("wrongodb-bench-{}-{}", std::process::id(), now_millis());

            let output = Command::new("docker")
                .arg("run")
                .arg("--rm")
                .arg("-d")
                .arg("--name")
                .arg(&name)
                .arg("-p")
                .arg(format!("{port}:27017"))
                .arg(&self.image)
                .arg("--bind_ip_all")
                .output()
                .map_err(|e| format!("failed to invoke docker run: {e}"))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                if stderr.contains("port is already allocated") {
                    continue;
                }
                last_err = Some(stderr);
                break;
            }

            let uri = format!("mongodb://127.0.0.1:{port}");
            self.port = Some(port);
            self.container_name = Some(name.clone());
            self.uri = Some(uri.clone());

            if let Err(err) = wait_for_ready(&uri, Duration::from_secs(READY_TIMEOUT_SECS)).await {
                let logs = docker_logs_tail(&name, 30)
                    .unwrap_or_else(|_| "(logs unavailable)".to_string());
                let _ = self.cleanup();
                return Err(format!(
                    "MongoDB failed readiness probe at {uri}: {err}\nrecent logs:\n{logs}"
                ));
            }

            return Ok(());
        }

        Err(format!(
            "failed to start MongoDB container{}",
            last_err
                .as_deref()
                .map(|e| format!(": {e}"))
                .unwrap_or_default()
        ))
    }

    fn cleanup(&mut self) -> Result<(), String> {
        if let Some(name) = self.container_name.take() {
            let status = Command::new("docker")
                .arg("rm")
                .arg("-f")
                .arg(&name)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map_err(|e| format!("failed to invoke docker rm -f for {name}: {e}"))?;
            if !status.success() {
                return Err(format!("docker rm -f failed for container {name}"));
            }
        }
        self.port = None;
        self.uri = None;
        Ok(())
    }
}

fn print_usage_and_exit(exit_code: i32) -> ! {
    eprintln!(
        "Usage: bench_wire_ab [OPTIONS]\n\
         \n\
         Options:\n\
           --warmup-secs <N>     Warmup seconds (default: 15)\n\
           --measure-secs <N>    Measurement seconds (default: 60)\n\
           --concurrency <LIST>  Comma-separated clients (default: 1,4,8,16,32,64)\n\
           --mongo-image <IMG>   MongoDB Docker image (default: mongo:7.0)\n\
           --out-dir <PATH>      Output directory (default: ./target/benchmarks/wire_ab)\n\
           --scenario <NAME>     insert_unique | update_hotspot | all (default: all)\n\
           --repetitions <N>     Repetitions per point (default: 3)\n\
           --help, -h            Show help\n"
    );
    std::process::exit(exit_code);
}

fn parse_args() -> Result<BenchmarkConfig, String> {
    let default_out_dir = std::env::current_dir()
        .map_err(|e| format!("failed to read cwd: {e}"))?
        .join("target/benchmarks/wire_ab");

    let mut cfg = BenchmarkConfig {
        warmup_secs: DEFAULT_WARMUP_SECS,
        measure_secs: DEFAULT_MEASURE_SECS,
        concurrencies: parse_concurrency_list(DEFAULT_CONCURRENCY)?,
        mongo_image: DEFAULT_MONGO_IMAGE.to_string(),
        out_dir: default_out_dir,
        scenarios: vec![Scenario::InsertUnique, Scenario::UpdateHotspot],
        repetitions: DEFAULT_REPETITIONS,
    };

    let mut iter = std::env::args().skip(1).peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => print_usage_and_exit(0),
            "--warmup-secs" => {
                cfg.warmup_secs = parse_u64_flag("--warmup-secs", iter.next())?;
            }
            "--measure-secs" => {
                cfg.measure_secs = parse_u64_flag("--measure-secs", iter.next())?;
            }
            "--concurrency" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--concurrency requires a value".to_string())?;
                cfg.concurrencies = parse_concurrency_list(&value)?;
            }
            "--mongo-image" => {
                cfg.mongo_image = iter
                    .next()
                    .ok_or_else(|| "--mongo-image requires a value".to_string())?;
            }
            "--out-dir" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--out-dir requires a value".to_string())?;
                cfg.out_dir = PathBuf::from(value);
            }
            "--scenario" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--scenario requires a value".to_string())?;
                cfg.scenarios = Scenario::parse(&value)?;
            }
            "--repetitions" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--repetitions requires a value".to_string())?;
                cfg.repetitions = value
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --repetitions value '{value}': {e}"))?;
                if cfg.repetitions == 0 {
                    return Err("--repetitions must be > 0".to_string());
                }
            }
            _ if arg.starts_with("--warmup-secs=") => {
                cfg.warmup_secs = parse_u64(&arg["--warmup-secs=".len()..], "--warmup-secs")?;
            }
            _ if arg.starts_with("--measure-secs=") => {
                cfg.measure_secs = parse_u64(&arg["--measure-secs=".len()..], "--measure-secs")?;
            }
            _ if arg.starts_with("--concurrency=") => {
                cfg.concurrencies = parse_concurrency_list(&arg["--concurrency=".len()..])?;
            }
            _ if arg.starts_with("--mongo-image=") => {
                cfg.mongo_image = arg["--mongo-image=".len()..].to_string();
            }
            _ if arg.starts_with("--out-dir=") => {
                cfg.out_dir = PathBuf::from(&arg["--out-dir=".len()..]);
            }
            _ if arg.starts_with("--scenario=") => {
                cfg.scenarios = Scenario::parse(&arg["--scenario=".len()..])?;
            }
            _ if arg.starts_with("--repetitions=") => {
                let value = &arg["--repetitions=".len()..];
                cfg.repetitions = value
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --repetitions value '{value}': {e}"))?;
                if cfg.repetitions == 0 {
                    return Err("--repetitions must be > 0".to_string());
                }
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unknown option '{arg}'"));
            }
            _ => {
                return Err(format!("unexpected positional argument '{arg}'"));
            }
        }
    }

    Ok(cfg)
}

fn parse_u64_flag(flag: &str, value: Option<String>) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("{flag} requires a value"))?;
    parse_u64(&value, flag)
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|e| format!("invalid {flag} value '{value}': {e}"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be > 0"));
    }
    Ok(parsed)
}

fn parse_concurrency_list(value: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for part in value.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed = trimmed
            .parse::<usize>()
            .map_err(|e| format!("invalid concurrency value '{trimmed}': {e}"))?;
        if parsed == 0 {
            return Err("concurrency values must be > 0".to_string());
        }
        out.push(parsed);
    }
    if out.is_empty() {
        return Err("--concurrency must contain at least one value".to_string());
    }
    Ok(out)
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn find_free_port() -> Result<u16, std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn ensure_docker_available() -> Result<(), String> {
    let status = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| format!("docker is not available on PATH: {e}"))?;

    if !status.success() {
        return Err(
            "docker daemon is not reachable; start Docker Desktop/daemon and retry".to_string(),
        );
    }

    Ok(())
}

fn docker_logs_tail(name: &str, lines: usize) -> Result<String, String> {
    let output = Command::new("docker")
        .arg("logs")
        .arg("--tail")
        .arg(lines.to_string())
        .arg(name)
        .output()
        .map_err(|e| format!("failed to invoke docker logs: {e}"))?;

    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn wait_for_ready(uri: &str, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let res = async {
            let options = ClientOptions::parse(uri)
                .await
                .map_err(|e| format!("parse client options failed: {e}"))?;
            let client = Client::with_options(options)
                .map_err(|e| format!("create MongoDB client failed: {e}"))?;
            client
                .database("admin")
                .run_command(doc! {"ping": 1})
                .await
                .map_err(|e| format!("ping failed: {e}"))
        }
        .await;

        if res.is_ok() {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(format!("timeout after {}s", timeout.as_secs()))
}

async fn new_client(uri: &str) -> Result<Client, String> {
    let mut options = ClientOptions::parse(uri)
        .await
        .map_err(|e| format!("failed to parse client options for {uri}: {e}"))?;
    options.app_name = Some("wrongodb-wire-ab-bench".to_string());
    Client::with_options(options).map_err(|e| format!("failed to create client: {e}"))
}

async fn run_backend(
    backend: &mut BackendKind,
    config: &BenchmarkConfig,
    session_run_id: &str,
) -> Result<Vec<RunResult>, String> {
    backend.start().await?;

    let run_result = async {
        let uri = backend.ready_uri()?.to_string();
        let client = new_client(&uri).await?;
        let db = client.database(DB_NAME);

        let mut results = Vec::new();
        for scenario in &config.scenarios {
            for &concurrency in &config.concurrencies {
                for repetition in 0..config.repetitions {
                    let row_run_id = format!(
                        "{session_run_id}-{}-{}-c{}-r{}",
                        backend.name(),
                        scenario.as_str(),
                        concurrency,
                        repetition
                    );
                    println!(
                        "running backend={} scenario={} concurrency={} repetition={}",
                        backend.name(),
                        scenario.as_str(),
                        concurrency,
                        repetition + 1
                    );

                    let collection_name = format!(
                        "bench_{}_{}_{}_c{}_r{}",
                        session_run_id,
                        backend.name(),
                        scenario.as_str(),
                        concurrency,
                        repetition
                    );
                    let collection = db.collection::<Document>(&collection_name);
                    backend.prepare_dataset(&collection, *scenario).await?;

                    let metrics = run_workload(
                        collection,
                        *scenario,
                        concurrency,
                        Duration::from_secs(config.warmup_secs),
                        Duration::from_secs(config.measure_secs),
                    )
                    .await?;

                    results.push(RunResult {
                        run_id: row_run_id,
                        backend: backend.name().to_string(),
                        scenario: *scenario,
                        concurrency,
                        ops_total: metrics.ops_ok,
                        ops_per_sec: metrics.ops_ok as f64 / config.measure_secs as f64,
                        p50_ms: percentile_ms(&metrics.latencies_us, 0.50),
                        p95_ms: percentile_ms(&metrics.latencies_us, 0.95),
                        p99_ms: percentile_ms(&metrics.latencies_us, 0.99),
                        error_rate: if metrics.ops_ok + metrics.ops_err == 0 {
                            0.0
                        } else {
                            metrics.ops_err as f64 / (metrics.ops_ok + metrics.ops_err) as f64
                        },
                    });
                }
            }
        }

        Ok(results)
    }
    .await;

    let cleanup_result = backend.cleanup();

    match (run_result, cleanup_result) {
        (Ok(results), Ok(())) => Ok(results),
        (Ok(_), Err(clean_err)) => Err(format!(
            "benchmark succeeded but cleanup failed: {clean_err}"
        )),
        (Err(run_err), Ok(())) => Err(run_err),
        (Err(run_err), Err(clean_err)) => {
            Err(format!("{run_err}; cleanup also failed: {clean_err}"))
        }
    }
}

async fn run_workload(
    collection: Collection<Document>,
    scenario: Scenario,
    concurrency: usize,
    warmup: Duration,
    measure: Duration,
) -> Result<WorkerMetrics, String> {
    let barrier = Arc::new(Barrier::new(concurrency));
    let payload = Arc::new("x".repeat(1024));

    let mut handles = Vec::with_capacity(concurrency);
    for worker_id in 0..concurrency {
        let worker_collection = collection.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_payload = Arc::clone(&payload);

        handles.push(tokio::spawn(async move {
            let total = warmup + measure;
            worker_barrier.wait().await;
            let start = Instant::now();

            let mut worker = WorkerMetrics::default();
            let mut counter = 0_u64;

            loop {
                if start.elapsed() >= total {
                    break;
                }

                let op_start = Instant::now();
                let op_result = match scenario {
                    Scenario::InsertUnique => {
                        let doc = doc! {
                            "_id": format!("w{worker_id}-{counter}"),
                            "payload": worker_payload.as_str(),
                            "k": counter as i64,
                        };
                        counter = counter.saturating_add(1);
                        worker_collection.insert_one(doc).await.map(|_| ())
                    }
                    Scenario::UpdateHotspot => worker_collection
                        .update_one(doc! {"_id": "hot"}, doc! {"$inc": {"k": 1_i64}})
                        .await
                        .map(|_| ()),
                };
                let elapsed = op_start.elapsed();

                if start.elapsed() >= warmup {
                    worker
                        .latencies_us
                        .push(u32::try_from(elapsed.as_micros()).unwrap_or(u32::MAX));
                    if op_result.is_ok() {
                        worker.ops_ok = worker.ops_ok.saturating_add(1);
                    } else {
                        worker.ops_err = worker.ops_err.saturating_add(1);
                    }
                }
            }

            worker
        }));
    }

    let mut merged = WorkerMetrics::default();
    for handle in handles {
        let worker = handle
            .await
            .map_err(|e| format!("worker task join error: {e}"))?;
        merged.ops_ok = merged.ops_ok.saturating_add(worker.ops_ok);
        merged.ops_err = merged.ops_err.saturating_add(worker.ops_err);
        merged.latencies_us.extend(worker.latencies_us);
    }

    Ok(merged)
}

fn percentile_ms(values_us: &[u32], percentile: f64) -> f64 {
    if values_us.is_empty() {
        return 0.0;
    }

    let mut sorted = values_us.to_vec();
    sorted.sort_unstable();

    let rank = ((sorted.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[rank] as f64 / 1000.0
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

fn evaluate_gate(results: &[RunResult]) -> GateReport {
    if results.iter().any(|r| r.error_rate > 0.01) {
        return GateReport {
            classification: "INCONCLUSIVE".to_string(),
            wrongo_scale: 0.0,
            mongo_scale: 0.0,
            scale_gap: 0.0,
            notes: "error_rate > 1% observed in at least one measured point".to_string(),
        };
    }

    let wrongo_ops_1 = median_metric(results, "wrongodb", Scenario::InsertUnique, 1, |r| {
        r.ops_per_sec
    });
    let wrongo_ops_32 = median_metric(results, "wrongodb", Scenario::InsertUnique, 32, |r| {
        r.ops_per_sec
    });
    let wrongo_p95_1 = median_metric(results, "wrongodb", Scenario::InsertUnique, 1, |r| r.p95_ms);
    let wrongo_p95_32 = median_metric(results, "wrongodb", Scenario::InsertUnique, 32, |r| {
        r.p95_ms
    });

    let mongo_ops_1 = median_metric(results, "mongodb", Scenario::InsertUnique, 1, |r| {
        r.ops_per_sec
    });
    let mongo_ops_32 = median_metric(results, "mongodb", Scenario::InsertUnique, 32, |r| {
        r.ops_per_sec
    });

    let (wrongo_ops_1, wrongo_ops_32, wrongo_p95_1, wrongo_p95_32, mongo_ops_1, mongo_ops_32) =
        match (
            wrongo_ops_1,
            wrongo_ops_32,
            wrongo_p95_1,
            wrongo_p95_32,
            mongo_ops_1,
            mongo_ops_32,
        ) {
            (Some(a), Some(b), Some(c), Some(d), Some(e), Some(f))
                if a > 0.0 && c > 0.0 && e > 0.0 =>
            {
                (a, b, c, d, e, f)
            }
            _ => {
                return GateReport {
                    classification: "INCONCLUSIVE".to_string(),
                    wrongo_scale: 0.0,
                    mongo_scale: 0.0,
                    scale_gap: 0.0,
                    notes: "missing insert_unique points for concurrency 1 and/or 32".to_string(),
                }
            }
        };

    let wrongo_scale = wrongo_ops_32 / wrongo_ops_1;
    let mongo_scale = mongo_ops_32 / mongo_ops_1;
    let scale_gap = mongo_scale - wrongo_scale;
    let wrongo_p95_growth = wrongo_p95_32 / wrongo_p95_1;

    let classification = if wrongo_scale < 1.7 && mongo_scale >= 2.5 && wrongo_p95_growth >= 3.0 {
        "RECOMMEND_REFACTOR"
    } else if wrongo_scale >= 2.2 || scale_gap < 0.7 {
        "DEFER_REFACTOR"
    } else {
        "INCONCLUSIVE"
    };

    GateReport {
        classification: classification.to_string(),
        wrongo_scale,
        mongo_scale,
        scale_gap,
        notes: format!(
            "wrongo_scale={wrongo_scale:.3}, mongo_scale={mongo_scale:.3}, wrongo_p95_growth={wrongo_p95_growth:.3}, scale_gap={scale_gap:.3}"
        ),
    }
}

fn median_metric<F>(
    results: &[RunResult],
    backend: &str,
    scenario: Scenario,
    concurrency: usize,
    metric: F,
) -> Option<f64>
where
    F: Fn(&RunResult) -> f64,
{
    let mut values: Vec<f64> = results
        .iter()
        .filter(|r| r.backend == backend && r.scenario == scenario && r.concurrency == concurrency)
        .map(metric)
        .collect();
    median(&mut values)
}

fn to_csv_row(result: &RunResult) -> String {
    format!(
        "{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.6}",
        result.run_id,
        result.backend,
        result.scenario.as_str(),
        result.concurrency,
        result.ops_total,
        result.ops_per_sec,
        result.p50_ms,
        result.p95_ms,
        result.p99_ms,
        result.error_rate
    )
}

fn write_results_csv(path: &Path, results: &[RunResult]) -> Result<(), String> {
    let mut file = File::create(path)
        .map_err(|e| format!("failed to create results CSV '{}': {e}", path.display()))?;

    writeln!(
        file,
        "run_id,backend,scenario,concurrency,ops_total,ops_per_sec,p50_ms,p95_ms,p99_ms,error_rate"
    )
    .map_err(|e| format!("failed to write CSV header: {e}"))?;

    for row in results {
        writeln!(file, "{}", to_csv_row(row))
            .map_err(|e| format!("failed to write CSV row: {e}"))?;
    }

    Ok(())
}

fn write_gate_json(path: &Path, gate: &GateReport) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(gate)
        .map_err(|e| format!("failed to serialize gate report JSON: {e}"))?;
    fs::write(path, bytes)
        .map_err(|e| format!("failed to write gate JSON '{}': {e}", path.display()))
}

fn write_summary(
    path: &Path,
    config: &BenchmarkConfig,
    results: &[RunResult],
    gate: &GateReport,
) -> Result<(), String> {
    let mut grouped: BTreeMap<(Scenario, usize, String), Vec<&RunResult>> = BTreeMap::new();
    for row in results {
        grouped
            .entry((row.scenario, row.concurrency, row.backend.clone()))
            .or_default()
            .push(row);
    }

    let mut summary = String::new();
    summary.push_str("# Wire A/B Benchmark Summary\n\n");
    summary.push_str("## Configuration\n\n");
    summary.push_str(&format!("- warmup_secs: {}\n", config.warmup_secs));
    summary.push_str(&format!("- measure_secs: {}\n", config.measure_secs));
    summary.push_str(&format!("- concurrencies: {:?}\n", config.concurrencies));
    summary.push_str(&format!(
        "- scenarios: {:?}\n",
        config
            .scenarios
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
    ));
    summary.push_str(&format!("- repetitions: {}\n", config.repetitions));
    summary.push_str(&format!("- mongo_image: {}\n", config.mongo_image));

    summary.push_str("\n## Median Results\n\n");
    summary.push_str(
        "| scenario | concurrency | backend | median ops/s | median p95 ms | median error rate |\n",
    );
    summary.push_str("|---|---:|---|---:|---:|---:|\n");

    for ((scenario, concurrency, backend), rows) in grouped {
        let mut ops: Vec<f64> = rows.iter().map(|r| r.ops_per_sec).collect();
        let mut p95: Vec<f64> = rows.iter().map(|r| r.p95_ms).collect();
        let mut error: Vec<f64> = rows.iter().map(|r| r.error_rate).collect();

        let median_ops = median(&mut ops).unwrap_or(0.0);
        let median_p95 = median(&mut p95).unwrap_or(0.0);
        let median_error = median(&mut error).unwrap_or(0.0);

        summary.push_str(&format!(
            "| {} | {} | {} | {:.3} | {:.3} | {:.6} |\n",
            scenario.as_str(),
            concurrency,
            backend,
            median_ops,
            median_p95,
            median_error
        ));
    }

    summary.push_str("\n## Gate\n\n");
    summary.push_str(&format!("- classification: `{}`\n", gate.classification));
    summary.push_str(&format!("- wrongo_scale: {:.3}\n", gate.wrongo_scale));
    summary.push_str(&format!("- mongo_scale: {:.3}\n", gate.mongo_scale));
    summary.push_str(&format!("- scale_gap: {:.3}\n", gate.scale_gap));
    summary.push_str(&format!("- notes: {}\n", gate.notes));

    fs::write(path, summary)
        .map_err(|e| format!("failed to write summary '{}': {e}", path.display()))
}

async fn run_benchmarks(
    config: &BenchmarkConfig,
    session_run_id: &str,
) -> Result<Vec<RunResult>, String> {
    fs::create_dir_all(&config.out_dir).map_err(|e| {
        format!(
            "failed to create benchmark output dir '{}': {e}",
            config.out_dir.display()
        )
    })?;

    let wrongo_port =
        find_free_port().map_err(|e| format!("failed to allocate WrongoDB port: {e}"))?;
    let mut wrongo = BackendKind::Wrongo(WrongoBackend::new(&config.out_dir, wrongo_port));
    let mut mongo = BackendKind::Mongo(MongoBackend::new(config.mongo_image.clone()));

    let mut all = Vec::new();

    let mut wrongo_rows = run_backend(&mut wrongo, config, session_run_id).await?;
    all.append(&mut wrongo_rows);

    let mut mongo_rows = run_backend(&mut mongo, config, session_run_id).await?;
    all.append(&mut mongo_rows);

    Ok(all)
}

fn write_artifacts(config: &BenchmarkConfig, results: &[RunResult]) -> Result<GateReport, String> {
    let results_path = config.out_dir.join("results.csv");
    let gate_path = config.out_dir.join("gate.json");
    let summary_path = config.out_dir.join("summary.md");

    write_results_csv(&results_path, results)?;
    let gate = evaluate_gate(results);
    write_gate_json(&gate_path, &gate)?;
    write_summary(&summary_path, config, results, &gate)?;

    Ok(gate)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            print_usage_and_exit(2);
        }
    };

    let session_run_id = format!("run-{}", now_millis());
    let results = run_benchmarks(&config, &session_run_id)
        .await
        .map_err(|e| format!("benchmark failed: {e}"))?;

    let gate = write_artifacts(&config, &results)
        .map_err(|e| format!("failed writing benchmark artifacts: {e}"))?;

    println!("benchmark complete");
    println!("output directory: {}", config.out_dir.display());
    println!("gate classification: {}", gate.classification);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_row(
        backend: &str,
        concurrency: usize,
        ops_per_sec: f64,
        p95_ms: f64,
        error_rate: f64,
    ) -> RunResult {
        RunResult {
            run_id: format!("{backend}-{concurrency}"),
            backend: backend.to_string(),
            scenario: Scenario::InsertUnique,
            concurrency,
            ops_total: 1,
            ops_per_sec,
            p50_ms: 0.0,
            p95_ms,
            p99_ms: 0.0,
            error_rate,
        }
    }

    #[test]
    fn percentile_ms_uses_nearest_rank() {
        let values = vec![1_000_u32, 2_000, 3_000, 4_000];
        assert_eq!(percentile_ms(&values, 0.50), 2.0);
        assert_eq!(percentile_ms(&values, 0.95), 4.0);
        assert_eq!(percentile_ms(&values, 0.99), 4.0);
    }

    #[test]
    fn csv_row_has_expected_columns() {
        let row = mk_row("wrongodb", 4, 123.456, 7.8, 0.01);
        let csv = to_csv_row(&row);
        let columns: Vec<&str> = csv.split(',').collect();
        assert_eq!(columns.len(), 10);
        assert_eq!(columns[1], "wrongodb");
        assert_eq!(columns[2], "insert_unique");
    }

    #[test]
    fn gate_recommends_refactor_on_flat_wrongo_and_scaling_mongo() {
        let rows = vec![
            mk_row("wrongodb", 1, 100.0, 1.0, 0.0),
            mk_row("wrongodb", 1, 100.0, 1.0, 0.0),
            mk_row("wrongodb", 32, 150.0, 4.0, 0.0),
            mk_row("wrongodb", 32, 150.0, 4.0, 0.0),
            mk_row("mongodb", 1, 120.0, 1.0, 0.0),
            mk_row("mongodb", 1, 120.0, 1.0, 0.0),
            mk_row("mongodb", 32, 400.0, 1.5, 0.0),
            mk_row("mongodb", 32, 400.0, 1.5, 0.0),
        ];

        let gate = evaluate_gate(&rows);
        assert_eq!(gate.classification, "RECOMMEND_REFACTOR");
    }

    #[test]
    fn gate_defers_when_wrongo_scales_well() {
        let rows = vec![
            mk_row("wrongodb", 1, 100.0, 1.0, 0.0),
            mk_row("wrongodb", 32, 260.0, 1.7, 0.0),
            mk_row("mongodb", 1, 120.0, 1.0, 0.0),
            mk_row("mongodb", 32, 320.0, 1.4, 0.0),
        ];

        let gate = evaluate_gate(&rows);
        assert_eq!(gate.classification, "DEFER_REFACTOR");
    }

    #[test]
    fn gate_is_inconclusive_on_error_rate() {
        let rows = vec![
            mk_row("wrongodb", 1, 100.0, 1.0, 0.0),
            mk_row("wrongodb", 32, 150.0, 4.0, 0.02),
            mk_row("mongodb", 1, 120.0, 1.0, 0.0),
            mk_row("mongodb", 32, 400.0, 1.5, 0.0),
        ];

        let gate = evaluate_gate(&rows);
        assert_eq!(gate.classification, "INCONCLUSIVE");
    }

    #[tokio::test]
    #[ignore = "requires docker and release build; long-running smoke test"]
    async fn smoke_generates_artifacts() {
        if ensure_docker_available().is_err() {
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let config = BenchmarkConfig {
            warmup_secs: 1,
            measure_secs: 2,
            concurrencies: vec![1],
            mongo_image: DEFAULT_MONGO_IMAGE.to_string(),
            out_dir: tmp.path().join("out"),
            scenarios: vec![Scenario::InsertUnique],
            repetitions: 1,
        };

        let run_id = format!("smoke-{}", now_millis());
        let rows = run_benchmarks(&config, &run_id).await.unwrap();
        assert!(!rows.is_empty());

        let gate = write_artifacts(&config, &rows).unwrap();
        assert!(!gate.classification.is_empty());
        assert!(config.out_dir.join("results.csv").exists());
        assert!(config.out_dir.join("gate.json").exists());
        assert!(config.out_dir.join("summary.md").exists());
    }
}
