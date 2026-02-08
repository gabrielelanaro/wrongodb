use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use wrongodb::{snapshot_lock_stats, start_server, WrongoDB, WrongoDBConfig};

fn print_usage_and_exit(exit_code: i32) -> ! {
    eprintln!(
        "Usage: wrongodb-server [--addr <HOST:PORT>] [--port <PORT>] [--db-path <PATH>] [ADDR]\n\
         \n\
         Options:\n\
           --addr, -a   Full address to bind, e.g. 127.0.0.1:27017\n\
           --port, -p   Port to bind on 127.0.0.1\n\
           --db-path    Database directory path (default: test.db)\n\
           --help, -h   Show this help message\n\
         \n\
         Notes:\n\
           * If both --addr and --port are provided, --addr wins.\n\
           * Legacy positional ADDR is still supported.\n\
           * DB path can also be set via WRONGO_DB_PATH."
    );
    std::process::exit(exit_code);
}

struct ParsedArgs {
    addr_flag: Option<String>,
    port_flag: Option<String>,
    db_path_flag: Option<String>,
    positional_addr: Option<String>,
}

fn parse_args() -> ParsedArgs {
    let mut parsed = ParsedArgs {
        addr_flag: None,
        port_flag: None,
        db_path_flag: None,
        positional_addr: None,
    };

    let mut iter = std::env::args().skip(1).peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => print_usage_and_exit(0),
            "--addr" | "-a" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --addr requires a value");
                    print_usage_and_exit(2);
                });
                parsed.addr_flag = Some(value);
            }
            "--port" | "-p" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --port requires a value");
                    print_usage_and_exit(2);
                });
                parsed.port_flag = Some(value);
            }
            "--db-path" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --db-path requires a value");
                    print_usage_and_exit(2);
                });
                parsed.db_path_flag = Some(value);
            }
            _ if arg.starts_with("--addr=") => {
                parsed.addr_flag = Some(arg["--addr=".len()..].to_string());
            }
            _ if arg.starts_with("--port=") => {
                parsed.port_flag = Some(arg["--port=".len()..].to_string());
            }
            _ if arg.starts_with("--db-path=") => {
                parsed.db_path_flag = Some(arg["--db-path=".len()..].to_string());
            }
            _ if arg.starts_with('-') => {
                eprintln!("error: unknown option '{arg}'");
                print_usage_and_exit(2);
            }
            _ => {
                if parsed.positional_addr.is_none() {
                    parsed.positional_addr = Some(arg);
                } else {
                    eprintln!("error: unexpected extra argument '{arg}'");
                    print_usage_and_exit(2);
                }
            }
        }
    }

    parsed
}

fn server_addr(parsed: &ParsedArgs) -> String {
    if let Some(addr) = parsed.addr_flag.clone() {
        return addr;
    }
    if let Some(port) = parsed.port_flag.clone() {
        return format!("127.0.0.1:{port}");
    }
    if let Some(addr) = parsed.positional_addr.clone() {
        return addr;
    }
    if let Ok(addr) = std::env::var("WRONGO_ADDR") {
        if !addr.is_empty() {
            return addr;
        }
    }
    if let Ok(port) = std::env::var("WRONGO_PORT") {
        if !port.is_empty() {
            return format!("127.0.0.1:{port}");
        }
    }
    "127.0.0.1:27017".to_string()
}

fn db_path(parsed: &ParsedArgs) -> String {
    if let Some(path) = parsed.db_path_flag.clone() {
        return path;
    }
    if let Ok(path) = std::env::var("WRONGO_DB_PATH") {
        if !path.is_empty() {
            return path;
        }
    }
    "test.db".to_string()
}

fn lock_stats_path() -> Option<String> {
    if let Ok(path) = std::env::var("WRONGO_LOCK_STATS_PATH") {
        if !path.is_empty() {
            return Some(path);
        }
    }
    None
}

fn should_enable_lock_stats(path: Option<&str>) -> bool {
    if path.is_some() {
        return true;
    }
    if let Ok(value) = std::env::var("WRONGO_LOCK_STATS_ENABLED") {
        return value == "1" || value.eq_ignore_ascii_case("true");
    }
    false
}

fn spawn_lock_stats_reporter(path: String) {
    std::thread::spawn(move || {
        let out_path = std::path::PathBuf::from(path);
        if let Some(parent) = out_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp_path = out_path.with_extension("tmp");

        loop {
            let snapshot = snapshot_lock_stats();
            if let Ok(bytes) = serde_json::to_vec_pretty(&snapshot) {
                let _ = std::fs::write(&tmp_path, bytes);
                let _ = std::fs::rename(&tmp_path, &out_path);
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parsed = parse_args();
    let stats_path = lock_stats_path();
    let db = WrongoDB::open_with_config(
        db_path(&parsed),
        WrongoDBConfig::new().lock_stats_enabled(should_enable_lock_stats(stats_path.as_deref())),
    )?;
    if let Some(path) = stats_path {
        spawn_lock_stats_reporter(path);
    }
    let db = Arc::new(Mutex::new(db));
    let addr = server_addr(&parsed);
    start_server(&addr, db).await?;
    Ok(())
}
