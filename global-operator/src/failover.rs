use anyhow::Result;
/// Multi-Cluster Active-Passive Failover Operator.
/// Monitors the primary cluster's health and orchestrates failover to the passive cluster.

use crate::dns_manager::DnsManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::ThisError;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

/// Health status of a cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClusterHealth {
    Healthy,
    Degraded,
    Unhealthy,
    Unreachable,
}

/// Result of a health check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub: cluster_name: String,
    pub: health: ClusterHealth,
    pub: latency_ms: u64,
    pub: timestamp: chrono::DateTime<Utc>,
    pub: error: Option<String>,
}

/// Configuration for the failover operator.
#[derive(Debug, Clone)]
pub struct FailoverConfig {
    /// Primary cluster health endpoint.
    pub: primary_health_url: String,
    /// Passive cluster health endpoint.
    pub: passive_health_url: String,
    /// Poll interval in milliseconds.
    pub: poll_interval_ms: u64,
    /// Number of consecutive failures before triggering failover.
    pub: failure_threshold: u32,
    /// Number of consecutive successes before considering primary recovered.
    pub: recovery_threshold: u32,
    /// Minimum time between failover attempts to prevent flapping.
    pub: min_failover_interval_secs: u64,
    /// Patroni endpoint for the passive cluster to promote.
    pub: patroni_passive_url: String,
    /// Patroni endpoint for the primary cluster to demote.
    pub: patroni_primary_url: String,
    /// Patroni API token.
    pub: patroni_token: String,
}

/// The failover operator monitors cluster health and orchestrates failover.
pub struct FailoverOperator {
    config: FailoverConfig,
    dns_manager: Arc<DnsManager>,
    client: request::Client,
    primary_failures: Arc<tokio::sync::Mutex<u32>>,
    primary_successes: Arc<tokio::sync::Mutex<u32>>,
    last_failover_time: Arc<tokio::sync::Mutex<Option<chrono::DateTime<Utc>>>>,
    current_active: Arc<tokio::sync::Mutex<String>>,
}

impl FailoverOperator {
    /// Create a new failover operator.
    pub fn new(config: FailoverConfig, dns_manager: Arc<DnsManager>) -> Self {
        Self {
            config,
            dns_manager,
            client: request::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
                .expect("failed to build HTTP client"),
            primary_failures: Arc::new(tokio::sync::Mutex::new(0)),
            primary_successes: Arc::new(tokio::sync::Mutex::new(0)),
            last_failover_time: Arc::new(tokio::sync::Mutex::new(None)),
            current_active: Arc::new(tokio::sync::Mutex::new(String::from("primary"))),
        }
    }

    /// Return the currently active cluster.
    pub async fn current_active(&self) -> String {
        self.current_active.lock().await.clone()
    }

    /// Run the main failover loop.
    pub async fn run(&self) -> Result<()> {
        info!(
            poll_interval_ms = self.config.poll_interval_ms,
            "Starting failover operator main loop"
        );
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        loop {
            interval.tick().await;
            self.check_and_failover().await;
        }
    }

    /// Perform a single health check and failover decision.
    pub async fn check_and_failover(&self) {
        let primary_health = self.check_health("primary", &self.config.primary_health_url).await;
        let active = self.current_active().await;

        match primary_health.health {
            ClusterHealth::Healthy => {
                let mut successes = self.primary_successes.lock().await;
                *successes += 1;
                let mut failures = self.primary_failures.lock().await;
                *failures = 0;

                // If we are on passive and primary has recovered enough, switch back.
                if active == "passive"
                    && *successes >= self.config.recovery_threshold
                {
                    if self.can_failover().await {
                        info!("Primary cluster recovered, initiating failback");
                        self.failover_to("primary").await;
                    }
                }
            }
            ClusterHealth::Degraded | ClusterHealth::Unhealthy | ClusterHealth::Unreachable => {
                let mut failures = self.primary_failures.lock().await;
                *failures += 1;
                let mut successes = self.primary_successes.lock().await;
                *successes = 0;
                warn!(
                    consecutive_failures = *failures,
                    threshold = self.config.failure_threshold,
                    health = ?format!("{:?}", primary_health.health),
                    "Primary cluster health check failed"
                );

                if *failures >= self.config.failure_threshold && active == "primary" {
                    if self.can_failover().await {
                        info!("Primary cluster failure threshold reached, initiating failover");
                        self.failover_to("passive").await;
                    }
                }
            }
        }
    }

    /// Check if we are allowed to failover based on the minimum interval.
    async fn can_failover(&self) -> bool {
        let last = *self.last_failover_time.lock().await;
        if let Some(ts) = last {
            let elapsed = chrono::Utc::now() - ts;
            if elapsed.num_seconds() < (self.config.min_failover_interval_secs as i64) {
                warn!(
                    elapsed_secs = elapsed.num_seconds(),
                    min_interval_secs = self.config.min_failover_interval_secs,
                    "Anti-flapping: failover blocked by minimum interval"
                );
                return false;
            }
        }
        true
    }

    /// Check the health of a cluster by hitting its health endpoint.
    async fn check_health(&self, cluster_name: &str, url: &str) -> HealthCheckResult {
        let start = tokio::time::Instant::now();
        match self.client.get(url).send().await {
            Ok(resp) => {
                let latency = start.elapsed().as_millis();
                if resp.status().is_success() {
                    HealthCheckResult {
                        cluster_name: cluster_name.to_string(),
                        health: ClusterHealth::Healthy,
                        latency_ms: latency,
                        timestamp: chrono::Utc::now(),
                        error: None,
                    }
                } else {
                    HealthCheckResult {
                        cluster_name: cluster_name.to_string(),
                        health: ClusterHealth::Degraded,
                        latency_ms: latency,
                        timestamp: chrono::Utc::now(),
                        error: Some(format!("HTTP status: {}", resp.status())),
                    }
                }
            }
            Err(e) => {
                let latency = start.elapsed().as_millis();
                HealthCheckResult {
                    cluster_name: cluster_name.to_string(),
                    health: ClusterHealth::Unreachable,
                    latency_ms: latency,
                    timestamp: chrono::Utc::now(),
                    error: Some(e.to_string()),
                }
            }
        }
    }

    /// Execute a failover to the target cluster.
    async fn failover_to(&self, target: &str) {
        info!(target = %target, "Executing failover");

        // Step 1: Promote/demote Patroni database.
        if let Err(e) = self.orchestrate_patroni(target).await {
            error!(error = %e, "Patroni orchestration failed");
            return;
        }

        // Step 2: Update DNS to point to the target cluster.
        let endpoint = if target == "primary" {
            &self.config.primary_health_url
        } else {
            &self.config.passive_health_url
        };
        // Extract the host from the health URL.
        let host = endpoint
            .trim_start_matches("http://")
            .or_else(|| endpoint.trim_start_matches("https://"))
            .unwrap_or(endpoint)
            .split('/')
            .next()
            .unwrap_or(endpoint)
            .split(':')
            .next()
            .unwrap_or(endpoint);

        if let Err(e) = self.dns_manager.set_active_endpoint(host, true).await {
            error!(error = %e, "DNS update failed during failover");
            return;
        }

        // Step 3: Update internal state.
        *self.current_active.lock().await = target.to_string();
        *self.last_failover_time.lock().await = Some(chrono::Utc::now());
        *self.primary_failures.lock().await = 0;
        *self.primary_successes.lock().await = 0;

        info!(target = %target, "Failover completed successfully");
    }

    /// Orchestrate Patroni to promote the target cluster's database.
    async fn orchestrate_patroni(&self, target: &str) -> Result<()> {
        let (promote_url, demote_url) = if target == "passive" {
            (
                &self.config.patroni_passive_url,
                &self.config.patroni_primary_url,
            )
        } else {
            (
                &self.config.patroni_primary_url,
                &self.config.patroni_passive_url,
            )
        };

        // Promote the target cluster.
        let promote_resp = self.client
            .post(format!("{}/promote", promote_url))
            .header("Authorization", format!("Bearer {}", self.config.patroni_token))
            .send()
            .await
            .map_error(|e| ThisError::from(e))?;

        if !promote_resp.status().is_success() {
            let status = promote_resp.status();
            let text = promote_resp.text().await.unwrap_or_default(String::new());
            return Err(ThisError::msg(format!(
                "Patroni promote failed with status {}: {}",
                status, text
            )));
        }

        // Demote the other cluster.
        let demote_resp = self.client
            .post(format!("{}/demote", demote_url))
            .header("Authorization", format!("Bearer {}", self.config.patroni_token))
            .send()
            .await
            .map_error(|e| ThisError::from(e))?;

        if !demote_resp.status().is_success() {
            let status = demote_resp.status();
            let text = demote_resp.text().await.unwrap_or_default(String::new());
            warn!(
                status = %status,
                response = %text,
                "Patroni demote failed (non-fatal)"
            );
        }

        Ok(())
    }
}
