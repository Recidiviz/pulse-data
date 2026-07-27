# `raw-data-import-monitoring`

Cloud Monitoring IaC for the raw-data import effort (parent
[OBT-2245](https://linear.app/recidiviz/issue/OBT-2245), this component
[OBT-35087](https://linear.app/recidiviz/issue/OBT-35087)).

Provisions, per project:

- **`google_monitoring_dashboard.raw_data_import`** — "Raw Data Import — Pod
  Resources & Cluster Capacity", a three-row dashboard:
  - **Row 1 — Pod resource usage:** CPU/memory request-utilization and used-bytes
    for `raw-data-file-chunking` / `raw-data-chunk-normalization` pods, grouped by
    the `recidiviz.org/entrypoint` pod label (by-step) and the
    `recidiviz.org/state_code` pod label (by-state)
    (added in [OBT-35077](https://linear.app/recidiviz/issue/OBT-35077)), plus peak
    scorecards and pod concurrency.
  - **Row 2 — Cluster capacity:** node count and allocatable-vs-requested CPU/memory
    for the `recidiviz-pod-node` Autopilot NAP (`gk3-…-nap-…`) node pool.
  - **Row 3 — Failures & preemptions:** the two logs-based metrics below.
- **`google_logging_metric.raw_data_pod_preemptions`** — `Preempted` events for
  raw-data pods, the chunk-normalization flakiness signal.
- **`google_logging_metric.raw_data_pod_failed_scheduling`** — `FailedScheduling`
  events for raw-data pods, the node-provisioning-scramble signal.

Both logs-based metrics explicitly exclude the warm-pool placeholder pods
(`recidiviz/airflow/dags/utils/warm_pool.py`, `WARM_POOL_NAME_PREFIX = "warm-pool-"`)
via `NOT jsonPayload.involvedObject.name=~"^warm-pool-"` — their preemptions are
the pool doing its job, not the flakiness signal these metrics track.

## Deployed to

`recidiviz-staging` and `recidiviz-123`, via the matching stack manifests under
`recidiviz/tools/deploy/atmos/stacks/`.

## Deploying

```bash
atmos terraform plan raw-data-import-monitoring -s recidiviz-staging
atmos terraform apply raw-data-import-monitoring -s recidiviz-staging
# then the same with -s recidiviz-123
```
