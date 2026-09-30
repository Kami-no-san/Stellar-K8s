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
//! Topology subsystem: zone awareness, spread-constraint rule generation,
//! and live enforcement against StatefulSets.
//!
//! Submodules:
//!
//! - [`zone`]: dynamic zone-awareness helpers (fetch zone topology, resolve
//!   requested anti-affinity strength against real cluster topology)
//! - [`rules`]: pure rule generation (`ClusterTopology` detection,
//!   `TopologySpreadConstraint` builders, hard/soft anti-affinity helpers)
//! - [`enforcer`]: server-side-apply enforcement on live StatefulSets

pub mod enforcer;
pub mod rules;
pub mod zone;

pub use enforcer::{
    build_statefulset_patch, discover_cluster_topology, enforce_namespace, enforce_on_statefulset,
    EnforcementResult,
};
pub use rules::{
    build_rule_set, hard_host_anti_affinity, soft_host_anti_affinity, zone_node_affinity_terms,
    ClusterTopology, TopologyMode, TopologyRuleSet, TopologySpreadConstraint, WhenUnsatisfiable,
    MIN_ZONES_FOR_HARD_SPREAD,
};
pub use zone::{
    count_sibling_nodes, fetch_zone_topology, resolve_anti_affinity_strength, ZoneTopology,
    ZONE_LABEL,
};
