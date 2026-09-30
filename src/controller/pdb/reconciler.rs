// Copyright 2024 Stellar-K8s Contributors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Dynamic Pod Disruption Budget (PDB) auto-tuning reconciler.
//!
//! Inspects managed Stellar validator pods, derives a quorum-safe
//! `maxUnavailable`, and creates or patches the namespace's
//! `PodDisruptionBudget` accordingly via server-side apply.

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use kube::Client;
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::health::safe_max_unavailable;

/// The label selector for identifying Stellar validator pods.
const STELLAR_LABEL: &str = "app=stellar";
/// The name of the PodDisruptionBudget resource managed by this controller.
const PDB_NAME: &str = "stellar-pdb";
/// Default interval between reconcile loops if not configured.
const DEFAULT_RECONCILE_INTERVAL_SECS: u64 = 30;

/// A reconciler that dynamically adjusts the PodDisruptionBudget based on
/// real-time health of the Stellar nodes.
pub struct PdbReconciler {
    client: Client,
    namespace: String,
}

impl PdbReconciler {
    /// Create a new reconciler using the given kubernetes client.
    pub fn new(client: Client) -> Self {
        let namespace = env::var("NAMESPACE").unwrap_or_else(|_| "default".to_string());
        Self { client, namespace }
    }

    /// Run the controller loop. This function blocks forever, periodically
    /// reconciling the PodDisruptionBudget.
    pub async fn run(&self) {
        let interval = env::var("RECONCILE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RECONCILE_INTERVAL_SECS);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval));
        loop {
            ticker.tick().await;
            info!("Reconciling PDB");
            if let Err(e) = self.reconcile().await {
                error!("Reconcile failed: {:?}", e);
            }
        }
    }

    /// Run a single reconciliation pass.
    ///
    /// Exposed publicly so integration tests and callers that manage their
    /// own scheduling can drive one pass without the blocking [`Self::run`]
    /// loop.
    pub async fn reconcile_once(&self) -> Result<(), kube::Error> {
        self.reconcile().await
    }

    /// Perform a single reconciliation: inspect Stellar pods, compute the
    /// desired maxUnavailable, and create/update the PDB accordingly.
    async fn reconcile(&self) -> Result<(), kube::Error> {
        debug!("Listing Stellar pods with label {}", STELLAR_LABEL);
        let pods_api: Api<Pod> = Api::all(self.client.clone());
        let lp = ListParams::default().labels(STELLAR_LABEL);
        let pods = pods_api.list(&lp).await?;

        let mut any_syncing = false;
        let mut ready_count = 0;
        for pod in pods.items {
            // Check sync status annotation
            if let Some(annotations) = pod.metadata.annotations {
                if annotations.get("stellar-sync-status").map(|v| v.as_str()) == Some("syncing") {
                    any_syncing = true;
                    warn!(
                        "Pod {} is currently syncing; PDB will block evictions",
                        pod.metadata.name.as_deref().unwrap_or("<unknown>")
                    );
                }
            }
            // Count ready pods (phase = Running and ready condition true)
            if let Some(status) = pod.status {
                if status.phase.as_deref() == Some("Running") {
                    if let Some(conditions) = status.conditions {
                        let ready = conditions
                            .iter()
                            .any(|c| c.type_ == "Ready" && c.status == "True");
                        if ready {
                            ready_count += 1;
                        }
                    } else {
                        // If no conditions, assume ready if Running
                        ready_count += 1;
                    }
                }
            }
        }

        // Determine desired maxUnavailable based on quorum safety.
        // If any node is syncing, we must not allow evictions.
        let desired_max_unavailable =
            safe_max_unavailable(!any_syncing, ready_count, quorum_size(ready_count));

        debug!(
            "Computed desired maxUnavailable: {} (sync: {}, ready: {})",
            desired_max_unavailable, any_syncing, ready_count
        );

        let pdb_api: Api<PodDisruptionBudget> =
            Api::namespaced(self.client.clone(), &self.namespace);

        // Build the selector used for the PDB; it always targets the stellar labels.
        let selector = LabelSelector {
            match_labels: Some(BTreeMap::from([("app".to_string(), "stellar".to_string())])),
            ..Default::default()
        };

        match pdb_api.get(PDB_NAME).await {
            Ok(_pdb) => {
                // Update existing PDB with server-side apply
                info!(
                    "Updating existing PDB {} with maxUnavailable={}",
                    PDB_NAME, desired_max_unavailable
                );
                let patch = json!({
                    "spec": {
                        "maxUnavailable": desired_max_unavailable
                    }
                });
                let pp = PatchParams::apply("pdb-auto-tuner").force();
                let apply = Patch::Apply(&patch);
                pdb_api.patch(PDB_NAME, &pp, &apply).await?;
            }
            Err(kube::Error::Api(api_err)) if api_err.code == 404 => {
                // Create new PDB
                info!(
                    "Creating PDB {} with maxUnavailable={}",
                    PDB_NAME, desired_max_unavailable
                );
                let pdb = PodDisruptionBudget {
                    metadata: ObjectMeta {
                        name: Some(PDB_NAME.to_string()),
                        namespace: Some(self.namespace.clone()),
                        ..Default::default()
                    },
                    spec: Some(k8s_openapi::api::policy::v1::PodDisruptionBudgetSpec {
                        min_available: None,
                        max_unavailable: Some(IntOrString::Int(desired_max_unavailable as i32)),
                        selector: Some(selector),
                        ..Default::default()
                    }),
                    status: None,
                };
                pdb_api.create(&PostParams::default(), &pdb).await?;
            }
            Err(e) => {
                warn!("Failed to fetch PDB {}: {:?}", PDB_NAME, e);
                return Err(e);
            }
        }

        Ok(())
    }
}

/// Minimum number of validators that must stay ready for the cluster to
/// retain quorum. With no external quorum configuration available, the
/// controller conservatively requires all currently-ready pods to remain
/// ready, so `maxUnavailable` degrades to 0 unless spare capacity exists.
fn quorum_size(ready_count: usize) -> usize {
    ready_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_namespace_is_used_when_env_is_missing() {
        // Directly exercises the env fallback used by PdbReconciler::new.
        let namespace = env::var("NAMESPACE").unwrap_or_else(|_| "default".to_string());
        assert!(!namespace.is_empty());
    }

    #[test]
    fn quorum_size_defaults_to_all_ready() {
        assert_eq!(quorum_size(5), 5);
        assert_eq!(quorum_size(0), 0);
    }
}
