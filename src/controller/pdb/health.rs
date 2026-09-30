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
//! Quorum-safety arithmetic shared by the PDB auto-tuning controller.

/// Number of validator pods that may be evicted at once without breaking
/// SCP safety.
///
/// When the cluster is not healthy (`healthy == false`) — any managed
/// validator is still syncing, or quorum capacity is unknown — no eviction
/// is permitted at all.
pub fn safe_max_unavailable(healthy: bool, synced: usize, quorum: usize) -> usize {
    if !healthy || synced < quorum {
        return 0;
    }
    // Keep one spare validator beyond quorum size when capacity allows it,
    // so a single drain never takes the node below its quorum floor.
    synced.saturating_sub(quorum).min(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unhealthy_cluster_blocks_all_evictions() {
        assert_eq!(safe_max_unavailable(false, 4, 3), 0);
    }

    #[test]
    fn fewer_synced_nodes_than_quorum_blocks_all_evictions() {
        assert_eq!(safe_max_unavailable(true, 2, 3), 0);
    }

    #[test]
    fn healthy_cluster_with_headroom_allows_one_eviction() {
        assert_eq!(safe_max_unavailable(true, 4, 3), 1);
    }

    #[test]
    fn healthy_cluster_at_exact_quorum_blocks_evictions() {
        assert_eq!(safe_max_unavailable(true, 3, 3), 0);
    }
}
