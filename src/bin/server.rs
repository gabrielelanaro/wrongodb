use std::sync::Arc;

use wrongodb::{
    start_server_with_replication, Connection, ConnectionConfig, ReplicationConfig, ReplicationRole,
};

fn print_usage_and_exit(exit_code: i32) -> ! {
    eprintln!(
        "Usage: wrongodb-server [--addr <HOST:PORT>] [--port <PORT>] [--db-path <PATH>] [ADDR]\n\
         \n\
         Options:\n\
           --addr, -a   Full address to bind, e.g. 127.0.0.1:27017\n\
           --port, -p   Port to bind on 127.0.0.1\n\
           --db-path    Database directory path (default: test.db)\n\
           --role       Replication role: primary or secondary\n\
           --node-name  Stable node name used for replication progress\n\
           --sync-source MongoDB URI for the primary when role=secondary\n\
           --help, -h   Show this help message\n\
         \n\
         Notes:\n\
           * If both --addr and --port are provided, --addr wins.\n\
           * Legacy positional ADDR is still supported.\n\
           * DB path can also be set via WRONGO_DB_PATH.\n\
           * Replication settings can also be set via WRONGO_ROLE, WRONGO_NODE_NAME, and WRONGO_SYNC_SOURCE."
    );
    std::process::exit(exit_code);
}

struct ParsedArgs {
    addr_flag: Option<String>,
    port_flag: Option<String>,
    db_path_flag: Option<String>,
    role_flag: Option<String>,
    node_name_flag: Option<String>,
    sync_source_flag: Option<String>,
    positional_addr: Option<String>,
}

fn parse_args() -> ParsedArgs {
    let mut parsed = ParsedArgs {
        addr_flag: None,
        port_flag: None,
        db_path_flag: None,
        role_flag: None,
        node_name_flag: None,
        sync_source_flag: None,
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
            "--role" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --role requires a value");
                    print_usage_and_exit(2);
                });
                parsed.role_flag = Some(value);
            }
            "--node-name" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --node-name requires a value");
                    print_usage_and_exit(2);
                });
                parsed.node_name_flag = Some(value);
            }
            "--sync-source" => {
                let value = iter.next().unwrap_or_else(|| {
                    eprintln!("error: --sync-source requires a value");
                    print_usage_and_exit(2);
                });
                parsed.sync_source_flag = Some(value);
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
            _ if arg.starts_with("--role=") => {
                parsed.role_flag = Some(arg["--role=".len()..].to_string());
            }
            _ if arg.starts_with("--node-name=") => {
                parsed.node_name_flag = Some(arg["--node-name=".len()..].to_string());
            }
            _ if arg.starts_with("--sync-source=") => {
                parsed.sync_source_flag = Some(arg["--sync-source=".len()..].to_string());
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

fn replication_config(parsed: &ParsedArgs) -> ReplicationConfig {
    let role_raw = parsed
        .role_flag
        .clone()
        .or_else(|| std::env::var("WRONGO_ROLE").ok())
        .unwrap_or_else(|| "primary".to_string());
    let role = match role_raw.to_lowercase().as_str() {
        "primary" => ReplicationRole::Primary,
        "secondary" => ReplicationRole::Secondary,
        other => {
            eprintln!("error: unsupported --role value '{other}'");
            print_usage_and_exit(2);
        }
    };

    let node_name = parsed
        .node_name_flag
        .clone()
        .or_else(|| std::env::var("WRONGO_NODE_NAME").ok())
        .unwrap_or_else(|| match role {
            ReplicationRole::Primary => "node-1".to_string(),
            ReplicationRole::Secondary => "node-2".to_string(),
        });
    let sync_source_uri = parsed
        .sync_source_flag
        .clone()
        .or_else(|| std::env::var("WRONGO_SYNC_SOURCE").ok())
        .filter(|value| !value.is_empty());
    if role == ReplicationRole::Secondary && sync_source_uri.is_none() {
        eprintln!("error: secondary role requires --sync-source or WRONGO_SYNC_SOURCE");
        print_usage_and_exit(2);
    }

    ReplicationConfig {
        role,
        node_name,
        primary_hint: sync_source_uri.as_deref().map(sync_source_primary_hint),
        sync_source_uri,
        term: 1,
    }
}

fn sync_source_primary_hint(uri: &str) -> String {
    uri.strip_prefix("mongodb://")
        .unwrap_or(uri)
        .trim_end_matches('/')
        .to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parsed = parse_args();
    let path = db_path(&parsed);
    let config = ConnectionConfig::default();
    let conn = Connection::open(&path, config)?;
    let conn = Arc::new(conn);
    let addr = server_addr(&parsed);
    let replication = replication_config(&parsed);
    start_server_with_replication(&addr, conn, replication).await?;
    Ok(())
}
