# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tasks that pre-warm Kubernetes node capacity.

These functions create a number of low-priority placeholder pods to reserve
capacity for quick startup. When a burst of tasks is scheduled, GKE Autopilot
will have already provisioned the nodes.

The placeholder pods run under a negative PriorityClass, so real entrypoint pods
preempt them instantly instead of waiting on on-demand node provisioning.

Adopt it in any DAG by declaring *what* you want kept warm -- a set of
interchangeable entrypoints and how many run in parallel across them -- and wiring
the returned setup/teardown tasks around the burst:

    setup, teardown = build_warm_pool_setup_and_teardown(
        pool_name="raw-data-import",
        specs=[WarmPoolSpec(
            name="pre-import",
            entrypoint_class_names=[
                "RawDataFileChunkingEntrypoint",
                "RawDataChunkNormalizationEntrypoint",
            ],
            parallelism=48,
        )],
    )
    upstream >> setup >> the_burst >> teardown

Per-pod CPU/memory are read from the entrypoints' own `recidiviz_kubernetes_resources.yaml`
 entries (sized to the largest), so the placeholders mirror the real pods and can never drift out of sync.

References:
OBT-2245 `raw_data_chunk_normalization` preemption failure.
"""
import logging
import time

import attr
from airflow.configuration import conf
from airflow.decorators import task
from airflow.models.xcom_arg import XComArg
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.trigger_rule import TriggerRule
from kubernetes import client
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    COMPOSER_USER_WORKLOADS,
    USER_WORKLOAD_SEPARATION_SPEC,
    KubernetesEntrypointResourceAllocator,
)
from recidiviz.airflow.dags.utils.kubernetes_pod_compute_resource_limits import (
    KubernetesPodComputeResourceLimits,
)
from recidiviz.common import attr_validators

# Shared, cluster-scoped PriorityClass for all warm-pool placeholder pods. Its
# value is strictly below the default pod priority (0) so the real entrypoint pods
# always preempt the placeholders, never the reverse (the OBT-2245 failure mode).
WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS = k8s.V1PriorityClass(
    metadata=k8s.V1ObjectMeta(name="recidiviz-warm-pool-placeholder"),
    value=-10,
    global_default=False,
    # Placeholders only fill spare capacity / trigger node provisioning; they
    # must never preempt other pods.
    preemption_policy="Never",
    description="Low-priority placeholders that pre-warm recidiviz node capacity.",
)
WARM_POOL_PLACEHOLDER_POD_LABEL_NAME = "recidiviz.org/warm_pool"

# Standard prefix on every warm-pool placeholder pod name, across all pools/specs,
# so they can be identified/excluded by name (e.g. in Cloud Monitoring)
WARM_POOL_NAME_PREFIX = "warm-pool-"

# Minimal do-nothing container used to hold the reservation.
PAUSE_IMAGE = "registry.k8s.io/pause:3.10"


@attr.define(frozen=True, kw_only=True)
class WarmPoolSpec:
    """A set of interchangeable entrypoints that share a concurrency budget on the
    node pool, warmed by a number placeholder pods.

    The placeholders are sized to the per-dimension max of the entrypoints' resource
    limits, so one placeholder can host a pod of any of them. This is the correct
    model when the entrypoints draw from a *shared* parallelism ceiling (e.g. the
    per-DAG task ceiling) rather than peaking independently -- sizing to the
    bounding box reserves capacity for the worst-case mix without double-counting.
    """

    name: str = attr.ib(validator=attr_validators.is_str)
    """Short identifier for this warm-slot type; used in the placeholder pod
    name (must be unique within a pool)."""

    entrypoint_class_names: list[str] = attr.ib(
        validator=[attr_validators.is_non_empty_list, attr_validators.is_list_of(str)]
    )
    """Interchangeable entrypoints sharing the parallelism budget. Resources come
    from each entrypoint's recidiviz_kubernetes_resources.yaml entry."""

    parallelism: int = attr.ib(validator=attr_validators.is_positive_int)
    """Peak number of concurrent pods across ALL these entrypoints combined (they
    share this budget -- e.g. the per-DAG task ceiling)."""


def _kubernetes_hook() -> KubernetesHook:
    return KubernetesHook(config_file=conf.get("kubernetes", "config_file"))


def _entrypoint_resource_limits(
    entrypoint_class_name: str,
) -> KubernetesPodComputeResourceLimits:
    """Returns the resource limits configured for |entrypoint_class_name|, the same
    values the real pods request (Autopilot sets requests == limits)."""
    return KubernetesPodComputeResourceLimits.from_kubernetes_resource_requirements(
        KubernetesEntrypointResourceAllocator().get_resources(entrypoint_class_name)
    )


def _bounding_box_limits(
    entrypoint_class_names: list[str],
) -> KubernetesPodComputeResourceLimits:
    """Returns resource limits big enough to host a pod of any of
    |entrypoint_class_names|: the per-dimension max across their configured limits."""
    limits = [
        _entrypoint_resource_limits(entrypoint_class_name)
        for entrypoint_class_name in entrypoint_class_names
    ]
    return KubernetesPodComputeResourceLimits(
        cpu_cores=max(limit.cpu_cores for limit in limits),
        memory_bytes=max(limit.memory_bytes for limit in limits),
    )


def _warm_pool_name(pool_name: str, spec_name: str) -> str:
    """Returns an RFC 1123 name unique to a (pool, spec),
    used as the placeholder pods' name prefix and warm pool label"""
    return f"{WARM_POOL_NAME_PREFIX}{pool_name}-{spec_name}".lower()


def _create_priority_class_if_not_exists() -> None:
    """Creates the shared negative-priority PriorityClass if it does not exist."""
    scheduling_client = client.SchedulingV1Api(_kubernetes_hook().api_client)
    priority_class_name = WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS.metadata.name
    try:
        scheduling_client.create_priority_class(
            WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS
        )
        logging.info("Created PriorityClass [%s]", priority_class_name)
    except ApiException as e:
        if e.status != 409:  # 409 Conflict == already exists
            raise
        logging.info("PriorityClass [%s] already exists", priority_class_name)


def _warm_pool_pod(
    *,
    name: str,
    limits: KubernetesPodComputeResourceLimits,
    active_deadline_seconds: int,
) -> k8s.V1Pod:
    """Returns one placeholder pod sized to |limits| on the dedicated node pool.

    These are *bare* pods (no controller), so a placeholder preempted by a real
    pod is simply gone -- it is NOT recreated. This avoids the recreate-then-
    provision node churn a Deployment spec would cause during the burst.

    They carry an active deadline so the pool expires even if teardown never runs.
    """
    quantities = limits.to_kubernetes_quantities()
    resources = k8s.V1ResourceRequirements(requests=quantities, limits=quantities)
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            # Let the server assign a unique name.
            generate_name=f"{name}-",
            namespace=COMPOSER_USER_WORKLOADS,
            labels={WARM_POOL_PLACEHOLDER_POD_LABEL_NAME: name},
        ),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(name="pause", image=PAUSE_IMAGE, resources=resources)
            ],
            priority_class_name=WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS.metadata.name,
            # Not recreated when it exits/is preempted.
            restart_policy="Never",
            # Self-terminate after this long so an orphaned pool (teardown never
            # ran) can't hold capacity forever.
            active_deadline_seconds=active_deadline_seconds,
            # Evict instantly when preempted by a real pod.
            termination_grace_period_seconds=0,
            # Same workload-separation selector/toleration as the real entrypoint
            # pods so the placeholder pods warm the pool that real pods will land on.
            **USER_WORKLOAD_SEPARATION_SPEC,
        ),
    )


def _delete_warm_pool_pods(*, pool_name: str, spec: WarmPoolSpec) -> None:
    """Deletes all placeholder pods for (pool, spec)."""
    name = _warm_pool_name(pool_name, spec.name)
    _kubernetes_hook().core_v1_client.delete_collection_namespaced_pod(
        namespace=COMPOSER_USER_WORKLOADS,
        label_selector=f"{WARM_POOL_PLACEHOLDER_POD_LABEL_NAME}={name}",
    )
    logging.info("Deleted warm pool pods for [%s]", name)


def _desired_placeholder_count(spec: WarmPoolSpec) -> int:
    """Returns how many placeholder pods to create and expect Running for |spec|:
    never more than the DAG could actually run at once (the per-DAG task ceiling),
    since the pool is effectively capped by core.max_active_tasks_per_dag."""
    return min(
        spec.parallelism,
        int(conf.get("core", "max_active_tasks_per_dag")),
    )


def _create_warm_pool_pods(
    *, pool_name: str, spec: WarmPoolSpec, active_deadline_seconds: int
) -> None:
    """(Re)creates the spec's placeholder pods: clears any existing ones for this
    (pool, spec), then creates one fresh pod per desired placeholder, sized to the
    bounding box of the spec's entrypoints."""
    name = _warm_pool_name(pool_name, spec.name)
    limits = _bounding_box_limits(spec.entrypoint_class_names)
    core_client = _kubernetes_hook().core_v1_client
    _delete_warm_pool_pods(pool_name=pool_name, spec=spec)

    count = _desired_placeholder_count(spec)
    for _ in range(count):
        core_client.create_namespaced_pod(
            namespace=COMPOSER_USER_WORKLOADS,
            body=_warm_pool_pod(
                name=name,
                limits=limits,
                active_deadline_seconds=active_deadline_seconds,
            ),
        )
    logging.info("Created [%d] warm pool pods for [%s]", count, name)


# How often to poll placeholder readiness while waiting for the pool to warm.
_READY_POLL_SECONDS = 10


def _wait_until_warm(
    *,
    pool_name: str,
    specs: list[WarmPoolSpec],
    timeout_seconds: int,
) -> None:
    """Blocks until every spec has all its placeholder pods Running (i.e. the nodes
    are provisioned), or |timeout_seconds| elapses. Warming is best-effort: on
    timeout this logs a warning and returns rather than failing, so a slow warm-up
    never blocks the downstream burst (retries cover the rest).
    """
    core_client = _kubernetes_hook().core_v1_client
    desired_by_name = {
        _warm_pool_name(pool_name, spec.name): _desired_placeholder_count(spec)
        for spec in specs
    }
    deadline = time.monotonic() + timeout_seconds
    while True:
        not_ready = {}
        for name, desired in desired_by_name.items():
            pods = core_client.list_namespaced_pod(
                namespace=COMPOSER_USER_WORKLOADS,
                label_selector=f"{WARM_POOL_PLACEHOLDER_POD_LABEL_NAME}={name}",
            )
            running = [
                pod
                for pod in pods.items
                if pod.status and pod.status.phase == "Running"
            ]
            if len(running) < desired:
                not_ready[name] = f"{len(running)}/{desired}"
        if not not_ready:
            logging.info("Warm pool [%s] fully ready", pool_name)
            return
        if time.monotonic() >= deadline:
            logging.warning(
                "Warm pool [%s] not fully ready after %ds (%s); proceeding anyway",
                pool_name,
                timeout_seconds,
                not_ready,
            )
            return
        time.sleep(_READY_POLL_SECONDS)


def build_warm_pool_setup_and_teardown(
    *,
    pool_name: str,
    specs: list[WarmPoolSpec],
    pod_active_deadline_seconds: int,
    ready_timeout_seconds: int = 0,
) -> tuple[XComArg, XComArg]:
    """Returns a (setup, teardown) task pair for the given warm-pool specs.

    The setup creates each spec's placeholder pods; the teardown deletes them all.
    The teardown is an Airflow teardown task (ALL_DONE) so it always releases
    capacity but never masks an upstream failure in the DAG run state.

    |pod_active_deadline_seconds| bounds each placeholder's lifetime, so the pool
    self-expires even if the teardown never runs (set it comfortably above the
    expected run duration).

    If |ready_timeout_seconds| > 0, the setup blocks until the placeholders are
    ready (nodes provisioned), up to that timeout, so downstream work is
    guaranteed warm capacity. Warming is best-effort: a timeout logs a warning and
    proceeds rather than failing the run.
    """

    @task(task_id="scale_up_warm_pool")
    def scale_up_warm_pool() -> None:
        _create_priority_class_if_not_exists()
        for spec in specs:
            _create_warm_pool_pods(
                pool_name=pool_name,
                spec=spec,
                active_deadline_seconds=pod_active_deadline_seconds,
            )
        if ready_timeout_seconds > 0:
            _wait_until_warm(
                pool_name=pool_name,
                specs=specs,
                timeout_seconds=ready_timeout_seconds,
            )

    @task(task_id="scale_down_warm_pool", trigger_rule=TriggerRule.ALL_DONE)
    def scale_down_warm_pool() -> None:
        for spec in specs:
            _delete_warm_pool_pods(pool_name=pool_name, spec=spec)

    scale_up = scale_up_warm_pool()
    return scale_up, scale_down_warm_pool().as_teardown(setups=[scale_up])
