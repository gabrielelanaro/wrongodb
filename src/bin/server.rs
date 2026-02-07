use std::sync::Arc;

use tokio::sync::Mutex;
use wrongodb::{start_server, WrongoDB};

fn print_usage_and_exit(exit_code: i32) -> ! {
    eprintln!(
        "Usage: wrongodb-server [--addr <HOST:PORT>] [--port <PORT>] [ADDR]\n\
         \n\
         Options:\n\
           --addr, -a   Full address to bind, e.g. 127.0.0.1:27017\n\
           --port, -p   Port to bind on 127.0.0.1\n\
           --help, -h   Show this help message\n\
         \n\
         Notes:\n\
           * If both --addr and --port are provided, --addr wins.\n\
           * Legacy positional ADDR is still supported."
    );
    std::process::exit(exit_code);
}

fn parse_args() -> (Option<String>, Option<String>, Option<String>) {
    let mut addr_flag: Option<String> = None;
    let mut port_flag: Option<String> = None;
    let mut positional_addr: Option<String> = None;

    let mut iter = std::env::args().skip(1).peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => print_usage_and_exit(0),
            "--addr" | "-a" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --addr requires a value");
                    print_usage_and_exit(2);
                });
                addr_flag = Some(value);
            }
            "--port" | "-p" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --port requires a value");
                    print_usage_and_exit(2);
                });
                port_flag = Some(value);
            }
            _ if arg.starts_with("--addr=") => {
                addr_flag = Some(arg["--addr=".len()..].to_string());
            }
            _ if arg.starts_with("--port=") => {
                port_flag = Some(arg["--port=".len()..].to_string());
            }
            _ if arg.starts_with('-') => {
                eprintln!("error: unknown option '{arg}'");
                print_usage_and_exit(2);
            }
            _ => {
                if positional_addr.is_none() {
                    positional_addr = Some(arg);
                } else {
                    eprintln!("error: unexpected extra argument '{arg}'");
                    print_usage_and_exit(2);
                }
            }
        }
    }

    (addr_flag, port_flag, positional_addr)
}

fn server_addr() -> String {
    let (addr_flag, port_flag, positional_addr) = parse_args();

    if let Some(addr) = addr_flag {
        return addr;
    }
    if let Some(port) = port_flag {
        return format!("127.0.0.1:{port}");
    }
    if let Some(addr) = positional_addr {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = WrongoDB::open("test.db")?;
    let db = Arc::new(Mutex::new(db));
    let addr = server_addr();
    start_server(&addr, db).await?;
    Ok(())
}
