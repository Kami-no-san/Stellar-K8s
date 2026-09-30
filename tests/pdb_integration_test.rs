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
//! Integration test for the dynamic PDB auto-tuning reconciler.
//!
//! Run with:
//!   cargo test --test pdb_integration_test -- --ignored
//!
//! The suite is `#[ignore]`-annotated so it never fails CI in environments
//! without a live Kubernetes API (see issue #1140 for the silent
//! early-return pattern this replaces).

use k8s_openapi::api::core::v1::{Container, Namespace, Pod, PodSpec};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ApiResource, DynamicObject, EvictParams, GroupKindVersion, PostParams};
use kube::Client;

use stellar_k8s::controller::pdb::reconciler::PdbReconciler;

const NAMESPACE: &str = "pdb-integration-test";
const NODE_NAME: &str = "node0";
const POD_NAME: &str = "validator-node0";
const PDB_NAME: &str = "stellar-pdb";

#[tokio::test]
#[ignore = "requires a live Kubernetes cluster (KUBECONFIG or in-cluster config)"]
async fn pdb_blocks_eviction_during_sync() {
    let client = Client::try_default()
        .await
        .expect("KUBECONFIG or in-cluster config must be available for this test");

    // Create a temporary namespace for the test.
    create_namespace(&client).await;

    // Create a StellarNode object for the validator under test.
    create_stellar_node(&client, NODE_NAME).await;

    // Create a Pod that matches the PDB selector labels and is syncing.
    create_pod(&client, POD_NAME).await;

    // Run a single reconciler pass. It should create a PDB with
    // maxUnavailable=0 while the validator is still syncing.
    let reconciler = reconciler_for_namespace(NAMESPACE);
    reconciler.reconcile_once().await.expect("reconcile failed");

    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), NAMESPACE);
    let pdb = pdb_api
        .get(PDB_NAME)
        .await
        .expect("PDB should have been created by the reconciler");
    let max_unavailable = pdb
        .spec
        .as_ref()
        .expect("PDB must have a spec")
        .max_unavailable
        .clone();
    assert_eq!(
        max_unavailable,
        Some(IntOrString::Int(0)),
        "maxUnavailable should be 0 while syncing"
    );

    // Attempt to evict the pod. Should be blocked by the PDB.
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    let result = pod_api.evict(POD_NAME, &EvictParams::default()).await;
    assert!(
        result.is_err(),
        "eviction should be blocked while node is syncing"
    );

    // Mark the pod as ready and no longer syncing, then reconcile again:
    // the PDB should now allow a single eviction.
    set_pod_ready_and_synced(&client, POD_NAME).await;
    reconciler
        .reconcile_once()
        .await
        .expect("second reconcile failed");

    let pdb = pdb_api
        .get(PDB_NAME)
        .await
        .expect("PDB should still exist after the second reconcile");
    let max_unavailable = pdb
        .spec
        .as_ref()
        .expect("PDB must have a spec")
        .max_unavailable
        .clone();
    assert_eq!(
        max_unavailable,
        Some(IntOrString::Int(1)),
        "maxUnavailable should be 1 after sync completes"
    );

    // Eviction should now succeed.
    let result = pod_api.evict(POD_NAME, &EvictParams::default()).await;
    assert!(
        result.is_ok(),
        "eviction should be successful after sync completes"
    );
}

/// Build a reconciler that operates on the given namespace. The reconciler
/// reads its namespace from the `NAMESPACE` env var at construction time.
fn reconciler_for_namespace(namespace: &str) -> PdbReconciler {
    // SAFETY: single-threaded test setup, before any other thread reads it.
    std::env::set_var("NAMESPACE", namespace);
    PdbReconciler::new(Client::try_default().await.expect("kube client"))
}

async fn create_namespace(client: &Client) {
    let ns_api: Api<Namespace> = Api::all(client.clone());
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some(NAMESPACE.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    match ns_api.create(&PostParams::default(), &ns).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => panic!("failed to create namespace: {e}"),
    }
}

async fn create_stellar_node(client: &Client, name: &str) {
    let gvk = GroupKindVersion::gvk("stellar.org", "v1alpha1", "StellarNode");
    let api_resource = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> =
        Api::namespaced(client.clone(), NAMESPACE).with_api_resource(api_resource);

    let node = DynamicObject::new(
        name,
        &serde_json::json!({
            "apiVersion": "stellar.org/v1alpha1",
            "kind": "StellarNode",
            "spec": {
                "nodeType": "Validator",
                "replicas": 1
            }
        }),
    );
    match api.create(&PostParams::default(), &node).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => panic!("failed to create StellarNode: {e}"),
    }
}

async fn create_pod(client: &Client, name: &str) {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(NAMESPACE.to_string()),
            labels: Some(
                [
                    ("app".to_string(), "stellar".to_string()),
                    ("stellar.org/network".to_string(), "testnet".to_string()),
                    ("stellar.org/node".to_string(), NODE_NAME.to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            annotations: Some(
                [("stellar-sync-status".to_string(), "syncing".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "stellar-core".to_string(),
                image: Some("stellar/stellar-core:latest".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    match pod_api.create(&PostParams::default(), &pod).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => panic!("failed to create pod: {e}"),
    }
}

async fn set_pod_ready_and_synced(client: &Client, name: &str) {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    let patch = serde_json::json!({
        "metadata": {
            "annotations": { "stellar-sync-status": "synced" }
        },
        "status": {
            "phase": "Running",
            "conditions": [
                { "type": "Ready", "status": "True" }
            ]
        }
    });
    let patched: Pod = pod_api
        .patch(
            name,
            kube::api::PatchParams::apply("pdb-integration-test"),
            &kube::api::Patch::Merge(&patch),
        )
        .await
        .expect("failed to mark pod ready and synced");
    assert_eq!(
        patched.metadata.name.as_deref(),
        Some(name),
        "patched pod name mismatch"
    );
}
