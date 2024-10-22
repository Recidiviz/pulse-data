# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for kubernetes_pod_operator_task_groups.py"""
import os
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.airflow.dags.utils.kubernetes_pod_operator_task_groups import (
    push_kpo_mapped_task_output_from_gcs_to_xcom,
    push_kpo_task_output_from_gcs_to_xcom,
)


class TestKubernetesPodOperatorTaskGroups(unittest.TestCase):
    """Tests for kubernetes_pod_operator_task_groups.py"""

    def setUp(self) -> None:
        self.mock_output_handler = MagicMock()
        self.output_handler_patcher = patch(
            "recidiviz.airflow.dags.utils.kubernetes_pod_operator_task_groups.KubernetesPodOperatorTaskOutputHandler.create_kubernetes_pod_operator_task_output_handler_from_context",
            return_value=self.mock_output_handler,
        )
        self.output_handler_patcher.start()

        self.mock_fs = MagicMock()
        self.gcsfs_hook_patcher = patch(
            "recidiviz.airflow.dags.utils.kubernetes_pod_operator_task_groups.get_gcsfs_from_hook",
            return_value=self.mock_fs,
        )
        self.gcsfs_hook_patcher.start()

        os.environ["GCP_PROJECT"] = "recidiviz-testing"

    def tearDown(self) -> None:
        self.output_handler_patcher.stop()
        self.gcsfs_hook_patcher.stop()

    def test_push_kpo_task_output_from_gcs_to_xcom(self) -> None:
        self.mock_output_handler.read_serialized_task_output.return_value = (
            "test-output"
        )

        output = push_kpo_task_output_from_gcs_to_xcom.function(task_id="test")

        self.assertEqual(output, "test-output")
        self.mock_output_handler.delete_mapped_task_output.assert_called_once_with(
            "test"
        )

    def test_push_kpo_mapped_task_output_from_gcs_to_xcom(self) -> None:
        self.mock_output_handler.read_serialized_mapped_task_output.return_value = [
            "test-output-0",
            "test-output-1",
        ]

        output = push_kpo_mapped_task_output_from_gcs_to_xcom.function(task_id="test")

        self.assertEqual(output, ["test-output-0", "test-output-1"])
        self.mock_output_handler.delete_mapped_task_output.assert_called_once_with(
            "test"
        )

    def test_read_error_doesnt_delete(self) -> None:
        self.mock_output_handler.read_serialized_task_output.side_effect = ValueError

        with self.assertRaises(ValueError):
            push_kpo_task_output_from_gcs_to_xcom.function(task_id="test")

        self.mock_output_handler.delete_task_output.assert_not_called()

    def test_read_error_doesnt_delete_mapped(self) -> None:
        self.mock_output_handler.read_serialized_mapped_task_output.side_effect = (
            ValueError
        )

        with self.assertRaises(ValueError):
            push_kpo_mapped_task_output_from_gcs_to_xcom.function(task_id="test")

        self.mock_output_handler.delete_mapped_task_output.assert_not_called()
