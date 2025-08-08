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
"""Tests for kubernetes_pod_operator_task_output_writer.py"""
import unittest
from unittest.mock import MagicMock, call, patch

from recidiviz.cloud_storage.gcs_file_system import GCSBlobDoesNotExistError
from recidiviz.cloud_storage.gcs_file_system_impl import GCSFileSystemImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.kubernetes_pod_operator_task_output_handler import (
    KubernetesPodOperatorTaskOutputFilePathBuilder,
    KubernetesPodOperatorTaskOutputHandler,
)


class TestTaskOutputFilePathHandler(unittest.TestCase):
    """Tests for TaskOutputFilePathHandler"""

    def test_output_file_path_no_task_map_index(self) -> None:
        expected_path = "test-project-airflow-kubernetes-pod-operator-outputs/test-dag/test-branch/test-task/test-run/output.json"

        handler = KubernetesPodOperatorTaskOutputFilePathBuilder(
            project_id="test-project",
            dag_id="test-dag",
            run_id="test-run",
        )
        actual_path = handler.output_file_path(task_id="test-branch.test-task")

        self.assertEqual(expected_path, actual_path.abs_path())

    def test_output_file_path_with_task_map_index(self) -> None:
        expected_path = "test-project-airflow-kubernetes-pod-operator-outputs/test-dag/test-branch/test-task/test-run/output_0.json"

        handler = KubernetesPodOperatorTaskOutputFilePathBuilder(
            project_id="test-project",
            dag_id="test-dag",
            run_id="test-run",
        )
        actual_path = handler.output_file_path(
            task_id="test-branch.test-task", task_map_index="0"
        )

        self.assertEqual(expected_path, actual_path.abs_path())

    def test_output_file_path_prefix(self) -> None:
        expected_prefix = "test-dag/test-branch/test-task/test-run/output"

        handler = KubernetesPodOperatorTaskOutputFilePathBuilder(
            project_id="test-project",
            dag_id="test-dag",
            run_id="test-run",
        )
        actual_prefix = handler.output_file_subdirectory_path_prefix(
            task_id="test-branch.test-task"
        )

        self.assertEqual(expected_prefix, actual_prefix)


class TestKubernetesPodOperatorTaskOutputHandler(unittest.TestCase):
    """Tests for KubernetesPodOperatorTaskOutputHandler"""

    def setUp(self) -> None:
        self.mock_fs = MagicMock(spec=GCSFileSystemImpl)
        path_handler = KubernetesPodOperatorTaskOutputFilePathBuilder(
            project_id="test-project",
            dag_id="test-dag",
            run_id="test-run",
        )
        self.handler = KubernetesPodOperatorTaskOutputHandler(
            fs=self.mock_fs, file_path_handler=path_handler
        )

    def _mock_open(self, return_strs: list[str]) -> MagicMock:
        mock_f = MagicMock()
        mock_f.read.side_effect = return_strs
        mock_open = MagicMock()
        mock_open.__enter__.return_value = mock_f
        return mock_open

    @patch(
        "recidiviz.utils.kubernetes_pod_operator_task_output_handler.in_airflow_kubernetes_pod",
        return_value=True,
    )
    @patch(
        "recidiviz.utils.kubernetes_pod_operator_task_output_handler.AirflowKubernetesPodEnvironment.get_task_id",
        return_value="task",
    )
    @patch(
        "recidiviz.utils.kubernetes_pod_operator_task_output_handler.AirflowKubernetesPodEnvironment.get_map_index",
        return_value=None,
    )
    def test_write_task_output(
        self,
        _mock_map_index: MagicMock,
        _mock_task_id: MagicMock,
        _mock_in_kube: MagicMock,
    ) -> None:
        self.handler.write_serialized_task_output(output_str="task-output")

        self.mock_fs.upload_from_string.assert_called_once()

    def test_read_mapped_task_output(self) -> None:
        self.mock_fs.ls.return_value = [
            GcsfsFilePath("test-bucket", "output_0.json"),
            GcsfsFilePath("test-bucket", "output_2.json"),
        ]
        self.mock_fs.open.return_value = self._mock_open(
            ["task-output-0", "task-output-1"]
        )

        result = self.handler.read_serialized_mapped_task_output("task")

        self.assertEqual(result, ["task-output-0", "task-output-1"])

    def test_read_mapped_task_output_no_files(self) -> None:
        self.mock_fs.ls.return_value = []

        with self.assertRaisesRegex(
            ValueError, "No mapped task output found for task_id"
        ):
            self.handler.read_serialized_mapped_task_output("task")

    def test_read_task_output(self) -> None:
        self.mock_fs.open.return_value = self._mock_open(["task-output"])

        result = self.handler.read_serialized_task_output("task")

        self.assertEqual(result, "task-output")

    def test_create_handler_from_env(self) -> None:
        with self.assertRaises(ValueError):
            KubernetesPodOperatorTaskOutputHandler.create_kubernetes_pod_operator_task_output_handler_from_pod_env(
                self.mock_fs
            )

    def test_delete_task_output(self) -> None:
        self.handler.delete_task_output("task")

        self.mock_fs.delete.assert_called_once()

    def test_delete_mapped_task_output(self) -> None:
        self.mock_fs.ls.return_value = [
            GcsfsFilePath("test-bucket", "output_0.json"),
            GcsfsFilePath("test-bucket", "output_1.json"),
        ]

        self.handler.delete_mapped_task_output("task")

        self.mock_fs.delete.assert_has_calls(
            [
                call(GcsfsFilePath("test-bucket", "output_0.json")),
                call(GcsfsFilePath("test-bucket", "output_1.json")),
            ]
        )

        self.assertEqual(self.mock_fs.delete.call_count, 2)

    def test_delete_file_doesnt_exist(self) -> None:
        self.mock_fs.delete.side_effect = GCSBlobDoesNotExistError("test-path")

        self.handler.delete_task_output("task")

        self.mock_fs.delete.assert_called_once()

    def test_delete_mapped_file_doesnt_exist(self) -> None:
        self.mock_fs.ls.return_value = [
            GcsfsFilePath("test-bucket", "output_0.json"),
            GcsfsFilePath("test-bucket", "output_1.json"),
        ]

        def mock_delete(file_path: GcsfsFilePath) -> None:
            if file_path.abs_path() == "test-path/output_0.json":
                raise GCSBlobDoesNotExistError(file_path.abs_path())

        self.mock_fs.delete.side_effect = mock_delete

        self.handler.delete_mapped_task_output("task")

        self.assertEqual(self.mock_fs.delete.call_count, 2)
