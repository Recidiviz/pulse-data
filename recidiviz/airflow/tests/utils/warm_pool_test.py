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
"""Tests for the reusable warm-pool tasks."""
# This module exercises warm_pool's module-private helpers directly.
# pylint: disable=protected-access
import unittest
from unittest.mock import MagicMock, patch

from airflow.decorators import dag
from airflow.models.xcom_arg import PlainXComArg
from kubernetes.client.rest import ApiException

from recidiviz.airflow.dags.utils import warm_pool
from recidiviz.airflow.dags.utils.kubernetes_pod_compute_resource_limits import (
    KubernetesPodComputeResourceLimits,
)
from recidiviz.airflow.dags.utils.warm_pool import (
    WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS,
    WarmPoolSpec,
    build_warm_pool_setup_and_teardown,
)

_NORMALIZATION = "RawDataChunkNormalizationEntrypoint"
_CHUNKING = "RawDataFileChunkingEntrypoint"
# "1.75Gi" rendered into whole bytes, as to_kubernetes_quantities() emits it.
_NORMALIZATION_MEMORY_BYTES = str(round(1.75 * 2**30))
# A spec covering both pre-import entrypoints, as the raw data DAG uses it.
_SPEC = WarmPoolSpec(
    name="pre-import",
    entrypoint_class_names=[_CHUNKING, _NORMALIZATION],
    parallelism=4,
)


class TestWarmPool(unittest.TestCase):
    """Tests for warm_pool helpers."""

    def setUp(self) -> None:
        self.hook = MagicMock()
        self.core_client = self.hook.core_v1_client
        self.hook_patcher = patch.object(
            warm_pool, "_kubernetes_hook", return_value=self.hook
        )
        self.hook_patcher.start()
        self.addCleanup(self.hook_patcher.stop)

    def test_entrypoint_resource_limits_reads_from_yaml(self) -> None:
        # Derived from recidiviz_kubernetes_resources.yaml -> no drift from the
        # real pods.
        self.assertEqual(
            KubernetesPodComputeResourceLimits(
                cpu_cores=1.0, memory_bytes=1.75 * 2**30
            ),
            warm_pool._entrypoint_resource_limits(_NORMALIZATION),
        )

    def test_warm_pool_name_is_rfc1123(self) -> None:
        name = warm_pool._warm_pool_name("raw-data-import", "pre-import")
        self.assertEqual("warm-pool-raw-data-import-pre-import", name)
        self.assertEqual(name, name.lower())

    def test_bounding_box_limits_takes_per_dimension_max(self) -> None:
        # chunking 1000m/1Gi, normalization 1000m/1.75Gi -> CPU tie, memory max.
        self.assertEqual(
            KubernetesPodComputeResourceLimits(
                cpu_cores=1.0, memory_bytes=1.75 * 2**30
            ),
            warm_pool._bounding_box_limits([_CHUNKING, _NORMALIZATION]),
        )

    def test_create_pods_clears_then_creates_bounding_box_placeholders(self) -> None:
        warm_pool._create_warm_pool_pods(
            pool_name="raw-data-import", spec=_SPEC, active_deadline_seconds=7200
        )
        # Existing placeholders cleared first, then one fresh pod per parallelism.
        self.core_client.delete_collection_namespaced_pod.assert_called_once()
        self.assertEqual(
            _SPEC.parallelism, self.core_client.create_namespaced_pod.call_count
        )
        pod = self.core_client.create_namespaced_pod.call_args.kwargs["body"]
        # Server-assigned name (never a deterministic one) so a recreate can't 409
        # against a still-Terminating pod; the label is what selectors match.
        self.assertEqual(
            "warm-pool-raw-data-import-pre-import-", pod.metadata.generate_name
        )
        self.assertIsNone(pod.metadata.name)
        self.assertEqual(
            {"recidiviz.org/warm_pool": "warm-pool-raw-data-import-pre-import"},
            pod.metadata.labels,
        )
        pod_spec = pod.spec
        # Bare, self-expiring placeholder with the workload-separation placement.
        self.assertEqual("Never", pod_spec.restart_policy)
        self.assertEqual(7200, pod_spec.active_deadline_seconds)
        self.assertEqual({"recidiviz-pod-node": "true"}, pod_spec.node_selector)
        self.assertEqual(
            WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS.metadata.name,
            pod_spec.priority_class_name,
        )
        self.assertEqual("recidiviz-pod-node", pod_spec.tolerations[0].key)
        # Sized to the bounding box of both entrypoints.
        resources = pod_spec.containers[0].resources
        expected = {"cpu": "1000m", "memory": _NORMALIZATION_MEMORY_BYTES}
        self.assertEqual(expected, resources.requests)
        self.assertEqual(expected, resources.limits)

    def test_delete_pods_deletes_by_label(self) -> None:
        warm_pool._delete_warm_pool_pods(pool_name="raw-data-import", spec=_SPEC)
        self.core_client.delete_collection_namespaced_pod.assert_called_once()
        kwargs = self.core_client.delete_collection_namespaced_pod.call_args.kwargs
        self.assertEqual(
            "recidiviz.org/warm_pool=warm-pool-raw-data-import-pre-import",
            kwargs["label_selector"],
        )

    def test_create_priority_class_if_not_exists_creates(self) -> None:
        scheduling_client = MagicMock()
        with patch.object(
            warm_pool.client, "SchedulingV1Api", return_value=scheduling_client
        ):
            warm_pool._create_priority_class_if_not_exists()
        scheduling_client.create_priority_class.assert_called_once_with(
            WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS
        )
        # Sanity-check the shared constant: a negative, non-preempting priority.
        self.assertLess(WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS.value, 0)
        self.assertEqual(
            "Never", WARM_POOL_PLACEHOLDER_POD_PRIORITY_CLASS.preemption_policy
        )

    def test_create_priority_class_if_not_exists_swallows_conflict(self) -> None:
        scheduling_client = MagicMock()
        scheduling_client.create_priority_class.side_effect = ApiException(status=409)
        with patch.object(
            warm_pool.client, "SchedulingV1Api", return_value=scheduling_client
        ):
            warm_pool._create_priority_class_if_not_exists()  # does not raise

    def test_spec_rejects_non_positive_parallelism(self) -> None:
        with self.assertRaises(ValueError):
            WarmPoolSpec(
                name="x", entrypoint_class_names=[_NORMALIZATION], parallelism=0
            )

    def test_spec_rejects_empty_entrypoints(self) -> None:
        with self.assertRaises(ValueError):
            WarmPoolSpec(name="x", entrypoint_class_names=[], parallelism=4)

    def _pod_list_with_running(self, running: int) -> MagicMock:
        pod = MagicMock()
        pod.status.phase = "Running"
        pod_list = MagicMock()
        pod_list.items = [pod] * running
        return pod_list

    def test_wait_until_warm_returns_when_ready(self) -> None:
        self.core_client.list_namespaced_pod.return_value = self._pod_list_with_running(
            4
        )
        with patch.object(warm_pool.time, "sleep") as mock_sleep:
            warm_pool._wait_until_warm(
                pool_name="raw-data-import",
                specs=[
                    WarmPoolSpec(
                        name="x", entrypoint_class_names=[_NORMALIZATION], parallelism=4
                    )
                ],
                timeout_seconds=60,
            )
        mock_sleep.assert_not_called()

    def test_wait_until_warm_gives_up_after_timeout(self) -> None:
        # Never reaches the desired running count.
        self.core_client.list_namespaced_pod.return_value = self._pod_list_with_running(
            1
        )
        # monotonic: first call (deadline) = 0, then a value past the timeout so we
        # poll once and then give up (best-effort, no raise).
        with patch.object(warm_pool.time, "sleep"), patch.object(
            warm_pool.time, "monotonic", side_effect=[0.0, 5.0, 100.0]
        ):
            warm_pool._wait_until_warm(
                pool_name="raw-data-import",
                specs=[
                    WarmPoolSpec(
                        name="x", entrypoint_class_names=[_NORMALIZATION], parallelism=4
                    )
                ],
                timeout_seconds=60,
            )

    def test_create_and_wait_agree_when_parallelism_exceeds_cap(self) -> None:
        # parallelism (48) exceeds the test config's max_active_tasks_per_dag (16),
        # so both paths must use the capped count -- otherwise the readiness gate
        # waits for a count that is never created and always times out.
        spec = WarmPoolSpec(
            name="x", entrypoint_class_names=[_NORMALIZATION], parallelism=48
        )
        capped_count = 16

        warm_pool._create_warm_pool_pods(
            pool_name="raw-data-import", spec=spec, active_deadline_seconds=7200
        )
        self.assertEqual(
            capped_count, self.core_client.create_namespaced_pod.call_count
        )

        # The gate is satisfied by the capped count, not the raw parallelism.
        self.core_client.list_namespaced_pod.return_value = self._pod_list_with_running(
            capped_count
        )
        with patch.object(warm_pool.time, "sleep") as mock_sleep:
            warm_pool._wait_until_warm(
                pool_name="raw-data-import", specs=[spec], timeout_seconds=60
            )
        mock_sleep.assert_not_called()

        # One short of the capped count is not ready: it polls then gives up.
        self.core_client.list_namespaced_pod.return_value = self._pod_list_with_running(
            capped_count - 1
        )
        with patch.object(warm_pool.time, "sleep") as mock_sleep, patch.object(
            warm_pool.time, "monotonic", side_effect=[0.0, 5.0, 100.0]
        ):
            warm_pool._wait_until_warm(
                pool_name="raw-data-import", specs=[spec], timeout_seconds=60
            )
        mock_sleep.assert_called_once()


class TestBuildWarmPoolTasks(unittest.TestCase):
    """Tests for the build_warm_pool_setup_and_teardown factory."""

    def test_returns_setup_and_teardown_tasks(self) -> None:
        captured: dict[str, PlainXComArg] = {}

        @dag(dag_id="warm_pool_test_dag", schedule=None)
        def _dag() -> None:
            setup, teardown = build_warm_pool_setup_and_teardown(
                pool_name="raw-data-import",
                specs=[
                    WarmPoolSpec(
                        name="x",
                        entrypoint_class_names=[_NORMALIZATION],
                        parallelism=48,
                    )
                ],
                pod_active_deadline_seconds=7200,
            )
            # Concrete XComArg subclass that exposes the underlying operator.
            assert isinstance(setup, PlainXComArg)
            assert isinstance(teardown, PlainXComArg)
            captured["setup"] = setup
            captured["teardown"] = teardown

        _dag()
        self.assertEqual("scale_up_warm_pool", captured["setup"].operator.task_id)
        self.assertEqual("scale_down_warm_pool", captured["teardown"].operator.task_id)
        # The teardown must not mask an upstream failure in the DAG run state.
        self.assertTrue(captured["teardown"].operator.is_teardown)
