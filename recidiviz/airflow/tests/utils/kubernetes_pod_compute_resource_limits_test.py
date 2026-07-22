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
"""Tests for KubernetesPodComputeResourceLimits."""
import unittest

from kubernetes.client import models as k8s

from recidiviz.airflow.dags.utils.kubernetes_pod_compute_resource_limits import (
    KubernetesPodComputeResourceLimits,
)


class TestKubernetesPodComputeResourceLimits(unittest.TestCase):
    """Tests for parsing and rendering Kubernetes resource quantities."""

    def test_from_requirements_parses_millicores_and_gibibytes(self) -> None:
        self.assertEqual(
            KubernetesPodComputeResourceLimits(
                cpu_cores=1.0, memory_bytes=1.75 * 2**30
            ),
            KubernetesPodComputeResourceLimits.from_kubernetes_resource_requirements(
                k8s.V1ResourceRequirements(limits={"cpu": "1000m", "memory": "1.75Gi"})
            ),
        )

    def test_from_requirements_parses_whole_core_and_mebibytes(self) -> None:
        self.assertEqual(
            KubernetesPodComputeResourceLimits(
                cpu_cores=2.0, memory_bytes=float(500 * 2**20)
            ),
            KubernetesPodComputeResourceLimits.from_kubernetes_resource_requirements(
                k8s.V1ResourceRequirements(limits={"cpu": "2", "memory": "500Mi"})
            ),
        )

    def test_from_requirements_raises_without_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, r"No resource limits set"):
            KubernetesPodComputeResourceLimits.from_kubernetes_resource_requirements(
                k8s.V1ResourceRequirements(limits=None)
            )

    def test_to_quantities_renders_millicores_and_bytes(self) -> None:
        limits = KubernetesPodComputeResourceLimits(
            cpu_cores=0.5, memory_bytes=float(2**30)
        )
        self.assertEqual(
            {"cpu": "500m", "memory": str(2**30)},
            limits.to_kubernetes_quantities(),
        )

    def test_round_trips_through_kubernetes_quantities(self) -> None:
        original = KubernetesPodComputeResourceLimits(
            cpu_cores=1.0, memory_bytes=1.75 * 2**30
        )
        round_tripped = (
            KubernetesPodComputeResourceLimits.from_kubernetes_resource_requirements(
                k8s.V1ResourceRequirements(limits=original.to_kubernetes_quantities())
            )
        )
        self.assertEqual(original, round_tripped)

    def test_rejects_non_positive_values(self) -> None:
        with self.assertRaises(ValueError):
            KubernetesPodComputeResourceLimits(cpu_cores=0.0, memory_bytes=2**30)
        with self.assertRaises(ValueError):
            KubernetesPodComputeResourceLimits(cpu_cores=1.0, memory_bytes=-1.0)
