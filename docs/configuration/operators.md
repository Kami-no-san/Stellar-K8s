# Operator Environment

This page covers the properties of the workloads the operator generates that are
**not** configurable from the `StellarNode` spec:

- [Pod hardening](#pod-hardening) — the security context and AppArmor
  annotations applied to every generated pod.

The operator's own startup configuration is a separate concern: it is the
`config.yaml` mounted at `/etc/stellar-operator/config.yaml` from the Helm
release's `operator-config` ConfigMap, located via the `STELLAR_OPERATOR_CONFIG`
environment variable on the operator Deployment. It does not affect the
workloads described on this page.

For the `StellarNode` fields, see [CRD Reference](crd-reference.md).

## Pod hardening

Every pod the operator generates carries a fixed security posture. None of it
is configurable from the `StellarNode` spec or from the operator's environment.

### Container security context

Applied to the main node container:

| Field | Value |
| --- | --- |
| `allowPrivilegeEscalation` | `false` |
| `capabilities.drop` | `["ALL"]` |
| `runAsNonRoot` | `true` |
| `privileged` | `false` |
| `readOnlyRootFilesystem` | `true` |
| `seccompProfile.type` | `RuntimeDefault` |

Applied at pod level:

| Field | Value |
| --- | --- |
| `runAsUser` / `runAsGroup` / `fsGroup` | `10000` |
| `runAsNonRoot` | `true` |
| `seccompProfile.type` | `RuntimeDefault` |

Two operational consequences:

- **`readOnlyRootFilesystem: true` means only mounted volumes are writable.** For
  a Validator that is `/opt/stellar/data`. Anything that needs to write elsewhere
  — a log file, a scratch directory, a cache — needs its own volume added via
  `spec.volumes` and `spec.volumeMounts`. See [Configuration](index.md).
- **Optional sidecars relax this.** The health-check sidecar, the eBPF exporter,
  and the snapshot-restore init container run with their own contexts, and the
  eBPF exporter is `privileged: true` because it loads BPF programs and reads
  `/sys/kernel/debug`. Enabling the eBPF exporter therefore requires a node pool
  that permits privileged containers.

### AppArmor

**There is no `STELLAR_APPARMOR_ENABLED` flag.** AppArmor annotations are applied
unconditionally to every generated pod; the operator has no environment variable
that gates them and no code path that omits them.

For every container and init container in the pod, the operator adds:

```
container.apparmor.security.beta.kubernetes.io/<container-name>: runtime/default
```

The annotations are set on the **pod** (`spec.template.metadata.annotations`) and
cover all containers, with a single exception: annotations injected for Vault
Agent seed delivery are merged in afterwards and can overwrite an entry.

Consequences to be aware of:

- **The annotation is the deprecated form.** The
  `container.apparmor.security.beta.kubernetes.io` prefix predates the
  `securityContext.appArmor` field added in Kubernetes 1.30. The operator emits
  the legacy prefix regardless of cluster version, so the pod relies on the
  kubelet still honouring it. On a cluster that has removed the deprecated
  annotation, pods may be rejected or admitted without an AppArmor profile.
  Pin to a cluster version that still supports it, and watch for removal in
  release notes for the version you run.
- **AppArmor must be available on the node.** The `runtime/default` profile
  requires the AppArmor LSM to be loaded on the node. On a cluster where AppArmor
  is unavailable — notably many managed container runtimes that ship with a
  `seccomp`-only or SELinux-only baseline — the annotation has no effect and the
  pod runs unconfined by AppArmor. This is silent: there is no admission
  rejection and no status condition.
- **To change the profile you must patch the workload, not the CR.** Because the
  operator re-renders the pod template on every reconcile, a `kubectl patch` of
  the annotations is reverted. Changing the profile requires an operator change.

Verify what is actually set on a running pod:

```bash
kubectl -n <namespace> get pod <node>-0 \
  -o jsonpath='{.spec.template.metadata.annotations}'
```

## Related

- [Configuration](index.md) — generated ConfigMap keys and writable paths
- [Storage](storage.md) — the one writable, persistent location
- [CRD Reference](crd-reference.md) — every `StellarNode` field
- [Pod Security Standards](../security/pss.md)
- [Container Image Security](../container-image-security.md)
