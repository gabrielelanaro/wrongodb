use std::process::Command;

use tempfile::tempdir;

#[test]
#[ignore = "requires docker and release benchmark run"]
fn bench_wire_ab_smoke_generates_artifacts() {
    let docker_ok = Command::new("docker")
        .arg("info")
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !docker_ok {
        return;
    }

    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("out");

    let status = Command::new("cargo")
        .arg("run")
        .arg("--release")
        .arg("--bin")
        .arg("bench_wire_ab")
        .arg("--")
        .arg("--warmup-secs")
        .arg("1")
        .arg("--measure-secs")
        .arg("2")
        .arg("--concurrency")
        .arg("1")
        .arg("--scenario")
        .arg("insert_unique")
        .arg("--repetitions")
        .arg("1")
        .arg("--out-dir")
        .arg(&out_dir)
        .status()
        .expect("failed to execute benchmark binary");

    assert!(status.success(), "benchmark command failed");
    assert!(out_dir.join("results.csv").exists());
    assert!(out_dir.join("gate.json").exists());
    assert!(out_dir.join("summary.md").exists());
}
