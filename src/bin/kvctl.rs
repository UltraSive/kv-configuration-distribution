//! Command-line client for a node's local Unix socket.

use clap::{Parser, Subcommand};
use kvdist::client::LocalClient;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "kvctl",
    version,
    about = "Talk to a kvdist node over its Unix socket"
)]
struct Cli {
    #[arg(long, env = "KV_SOCKET", default_value = "/tmp/kvdist.sock")]
    socket: PathBuf,
    /// Only answer once the node has applied this index.
    #[arg(long, default_value_t = 0)]
    min_index: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print a value to stdout (exit code 1 if absent).
    Get {
        key: String,
    },
    /// Set a value; `-` reads it from stdin.
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// List `key<TAB>value` lines under a prefix.
    Scan {
        #[arg(default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 1000)]
        limit: u32,
    },
    /// Node status as JSON.
    Status,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("kvctl: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let mut kv = LocalClient::connect(&cli.socket).await?;
    let mut out = std::io::stdout().lock();
    match cli.cmd {
        Cmd::Get { key } => {
            let (index, item) = kv.get_one(key, cli.min_index).await?;
            let Some(item) = item else {
                eprintln!("not found (index {index})");
                return Ok(ExitCode::from(1));
            };
            out.write_all(&item.value)?;
        }
        Cmd::Set { key, value } => {
            let value = if value == "-" {
                let mut buf = Vec::new();
                std::io::stdin().read_to_end(&mut buf)?;
                buf
            } else {
                value.into_bytes()
            };
            writeln!(out, "{}", kv.set(key, value).await?)?;
        }
        Cmd::Delete { key } => writeln!(out, "{}", kv.delete(key).await?)?,
        Cmd::Scan { prefix, limit } => {
            let (_, items) = kv.scan(prefix, None, limit, cli.min_index).await?;
            for (k, item) in items {
                out.write_all(&k)?;
                out.write_all(b"\t")?;
                out.write_all(&item.value)?;
                out.write_all(b"\n")?;
            }
        }
        Cmd::Status => writeln!(
            out,
            "{}",
            serde_json::to_string_pretty(&kv.status().await?)?
        )?,
    }
    Ok(ExitCode::SUCCESS)
}
