//! sensenova-proxy: a small robust SenseNova Token Plan gateway for Claude
//! Code. Native Anthropic Messages passthrough with gateway auth, model
//! aliasing, bounded retries, 429/quota handling, circuit breaking,
//! concurrency shaping, secret redaction, and stream validation.

mod auth;
mod circuit;
mod concurrency;
mod config;
mod error;
mod metrics;
mod models;
mod pool;
mod rate_limit;
mod redaction;
mod server;
mod sse;
mod upstream;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::config::{Config, LogFormat, default_config_path};

#[derive(Parser)]
#[command(
    name = "sensenova-proxy",
    version,
    about = "SenseNova Token Plan gateway for Claude Code (Anthropic Messages API)"
)]
struct Cli {
    /// JSON configuration path (or SENSENOVA_PROXY_CONFIG).
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(default_config_path);
    let config = Config::load(&config_path)?;
    init_tracing(&config);
    let enabled_keys = config
        .sensenova_api_keys
        .iter()
        .filter(|key| key.enabled)
        .count();
    tracing::info!(
        config_path = %config_path.display(),
        bind = %config.server.bind,
        upstream = %config.upstream.base_url,
        messages_path = %config.upstream.messages_path,
        upstream_model = %config.models.default,
        enabled_keys,
        max_attempts = config.retry.max_attempts,
        concurrency_limit = config.concurrency.initial,
        queue_capacity = config.concurrency.queue_capacity,
        "configuration loaded"
    );
    server::serve(server::AppState::new(config)?).await
}

fn init_tracing(config: &Config) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.runtime.log_level));
    match config.runtime.log_format {
        LogFormat::Pretty => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .compact()
                .try_init();
        }
        LogFormat::Json => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .json()
                .try_init();
        }
    }
}
