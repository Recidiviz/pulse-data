# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for entrypoint resource allocation"""
import os
import unittest
from typing import Any

import yaml
from kubernetes.client import models as k8s

import recidiviz
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    KubernetesEntrypointResourceAllocator,
)


class TestKubernetesResourceAllocator(unittest.TestCase):
    """Tests for KubernetesResourceAllocator."""

    entrypoint_args_fixture: Any

    @classmethod
    def setUpClass(cls) -> None:
        with open(
            os.path.join(
                os.path.dirname(recidiviz.__file__),
                "airflow/tests/fixtures/entrypoints_args.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as fixture_file:
            cls.entrypoint_args_fixture = yaml.safe_load(fixture_file)

    def test_known_entrypoints(self) -> None:
        allocator = KubernetesEntrypointResourceAllocator()

        for argv in self.entrypoint_args_fixture.values():
            self.assertIsNotNone(allocator.get_resources(argv))

    def test_resources(self) -> None:
        allocator = KubernetesEntrypointResourceAllocator()
        self.assertEqual(
            allocator.get_resources(argv=["--entrypoint=ValidationEntrypoint"]),
            k8s.V1ResourceRequirements(
                limits={"cpu": "250m", "memory": "1.5Gi"}, requests=None
            ),
        )

        self.assertEqual(
            allocator.get_resources(
                argv=[
                    "--entrypoint=MetricViewExportEntrypoint",
                    "--export_job_name=LANTERN",
                    "--state_code=US_PA",
                ]
            ),
            k8s.V1ResourceRequirements(
                limits={"cpu": "500m", "memory": "3Gi"}, requests=None
            ),
        )
