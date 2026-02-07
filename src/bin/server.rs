use std::sync::Arc;

use tokio::sync::Mutex;
use wrongodb::{start_server, WrongoDB};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parsed = parse_args();
    let db = WrongoDB::open(db_path(&parsed))?;
    let db = Arc::new(Mutex::new(db));
    let addr = server_addr(&parsed);
    start_server(&addr, db).await?;
    Ok(())
}
