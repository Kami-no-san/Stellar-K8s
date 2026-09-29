use anyhow::Result;
/// Multi-Cluster Active-Passive Failover Operator entry point.

mod dns_manager;
mod failover;

use clap::Parser;
use dns_manager::{DnsBackend, DnsManager, DnsManagerConfig};
use failover::{FailoverConfig, FailoverOperator};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug, Clone)]
struct Cli {
    /// DNS backend to use (route53 or cloudflare).
    #[arg(long, env = "DNS_BACKEND", default_value = "route53")]
    dns_backend: String,
    /// Global domain to manage.
    #[arg(long, env = "DNS_DOMAIN", default_value = "rpc.stellar.org")]
    dns_domain: String,
    /// DNS hosted zone ID.
    #[arg(long, env = "DNS_HOSTED_ZONE_ID")]
    dns_hosted_zone_id: String,
    /// DNS API token.
    #[arg(long, env = "DNS_API_TOKEN")]
    dns_api_token: String,
    /// Primary cluster health URL.
    #[arg(long, env = "PRIMARY_HEALTH_URL")]
    primary_health_url: String,
    /// Passive cluster health URL.
    #[arg(long, env = "PASSIVE_HEALTH_URL")]
    passive_health_url: String,
    /// Patroni primary endpoint.
    #[arg(long, env = "PATRONI_PRIMARY_URL")]
    patroni_primary_url: String,
    /// Patroni passive endpoint.
    #[arg(long, env = "PATRONI_PASSIVE_URL")]
    patroni_passive_url: String,
    /// Patroni API token.
    #[arg(long, env = "PATRONI_TOKEN")]
    patroni_token: String,
    /// Poll interval in milliseconds.
    #[arg(long, env = "POLL_INTERVAL_MS", default_value = "500")]
    poll_interval_ms: u64,
    /// Number of consecutive failures before failover.
    #[arg(long, env = "FAILURE_THRESHOLD", default_value = "3")]
    failure_threshold: u32,
    /// Number of consecutive successes before failback.
    #[arg(long, env = "RECOVERY_THRESHOLD", default_value = "10")]
    recovery_threshold: u32,
    /// Minimum time between failovers in seconds.
    #[arg(long, env = "MIN_FAILOVER_INTERVAL_SECS", default_value = "60")]
    min_failover_interval_secs: u64,
    /// DNS TTL in seconds.
    #[arg(long, env = "DNS_TTL", default_value = "60")]
    dns_ttl: u32,
    /// Minimum TTL between DNS updates in seconds.
    #[arg(long, env = "DNS_MIN_TTL", default_value = "30")]
    dns_min_ttl: u32,
}

#[tokio(::main)]
async fn main() -> Result<()> {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer::fmt::layer()
                .with_target(true)
                .with_thread_names(true),
        )
        .init();

    let cli = Cli::parse();
    info!(config = ?cli, "Starting global failover operator");

    let backend = match cli.dns_backend.to_lowercase().as_str() {
        "cloudflare" => DnsBackend::Cloudflare,
        "Route53" => DnsBackend::Route53,
        _ => {
            eprintln!("Unknown DNS backend: {}", cli.dns_backend);
            std::process::exit(1);
        }
    };

    let dns_config = DnsManagerConfig {
        backend,
        domain: cli.dns_domain.clone(),
        hosted_zone_id: cli.dns_hosted_zone_id.clone(),
        api_token: cli.dns_api_token.clone(),
        primary_endpoint: cli.primary_health_url.clone(),
        passive_endpoint: cli.passive_health_url.clone(),
        ttl_seconds: cli.dns_ttl,
        min_ttl_seconds: cli.dns_min_ttl,
    };

    let dns_manager = Arc::new(DnsManager::new(dns_config));

    let failover_config = FailoverConfig {
        primary_health_url: cli.primary_health_url.clone(),
        passive_health_url: cli.passive_health_url.clone(),
        poll_interval_ms: cli.poll_interval_ms,
        failure_threshold: cli.failure_threshold,
        recovery_threshold: cli.recovery_threshold,
        min_failover_interval_secs: cli.min_failover_interval_secs,
        patroni_passive_url: cli.patroni_passive_url.clone(),
        patroni_primary_url: cli.patroni_primary_url.clone(),
        patroni_token: cli.patroni_token.clone(),
    };

    let operator = Arc::new(FailoverOperator::new(failover_config, dns_manager));

    // Set initial DNS to primary.
    let primary_host = cli.primary_health_url
        .trim_start_matches("http://")
        .or_else(|| cli.primary_health_url.trim_start_matches("https://"))
        .unwrap_or(&cli.primary_health_url)
        .split('/')
        .next()
        .unwrap_or(&cli.primary_health_url)
        .split(':')
        .next()
        .unwrap_or(&cli.primary_health_url);

    operator
        .dns_manager
        .set_active_endpoint(primary_host, true)
        .await?;

    // Run the operator loop.
    operator.run().await?

    Ok(())
}
