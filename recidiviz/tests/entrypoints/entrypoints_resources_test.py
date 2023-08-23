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
import argparse
import os
import unittest
from typing import Any

import yaml

import recidiviz
from recidiviz.entrypoints.entrypoint_executor import (
    ENTRYPOINTS_BY_NAME,
    parse_arguments,
)
from recidiviz.entrypoints.entrypoint_resources import (
    DEFAULT_RESOURCES,
    KubernetesEntrypointResourceAllocator,
)
from recidiviz.entrypoints.metric_export.metric_view_export import (
    MetricViewExportEntrypoint,
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
            executor_args, entrypoint_argv = parse_arguments(argv)
            entrypoint = ENTRYPOINTS_BY_NAME[executor_args.entrypoint]
            self.assertIsNotNone(
                allocator.get_resources(
                    executor_args=executor_args,
                    entrypoint_args=entrypoint.get_parser().parse_args(entrypoint_argv),
                )
            )

    def test_resources(self) -> None:
        allocator = KubernetesEntrypointResourceAllocator()
        args, _ = parse_arguments(["--entrypoint=ValidationEntrypoint"])
        self.assertEqual(
            allocator.get_resources(
                executor_args=args,
                entrypoint_args=argparse.Namespace(),
            ),
            DEFAULT_RESOURCES,
        )

        args, entrypoint_args = parse_arguments(
            [
                "--entrypoint=MetricViewExportEntrypoint",
                "--export_job_name=LANTERN",
                "--state_code=US_PA",
            ]
        )
        self.assertEqual(
            allocator.get_resources(
                executor_args=args,
                entrypoint_args=MetricViewExportEntrypoint.get_parser().parse_args(
                    entrypoint_args
                ),
            ),
            {
                "limits": {"cpu": "1000m", "memory": "3Gi"},
            },
        )
