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
"""Tests for RecidivizKubernetesPodOperator label helpers."""
import os
import unittest
from typing import Any
from unittest.mock import patch

import yaml
from kubernetes.client import models as k8s

import recidiviz
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    RECIDIVIZ_LABEL_PREFIX,
    RecidivizKubernetesPodOperator,
    labels_from_arguments,
)


class TestLabelsFromArguments(unittest.TestCase):
    """Tests for labels_from_arguments."""

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

    def test_allowlisted_args_become_labels(self) -> None:
        self.assertEqual(
            {
                "recidiviz.org/entrypoint": "MetricViewExportEntrypoint",
                "recidiviz.org/export_job_name": "LANTERN",
                "recidiviz.org/state_code": "US_PA",
            },
            labels_from_arguments(
                [
                    "--entrypoint=MetricViewExportEntrypoint",
                    "--export_job_name=LANTERN",
                    "--state_code=US_PA",
                ]
            ),
        )

    def test_serialized_and_unknown_args_excluded(self) -> None:
        # A realistic raw-data normalization argv: the entrypoint and state are
        # allowlisted, but the serialized chunk payload must never become a label.
        self.assertEqual(
            {
                "recidiviz.org/entrypoint": "RawDataChunkNormalizationEntrypoint",
                "recidiviz.org/state_code": "US_MI",
            },
            labels_from_arguments(
                [
                    "run",
                    "--entrypoint=RawDataChunkNormalizationEntrypoint",
                    "--state_code=US_MI",
                    "--file_chunks=path/one.csv###path/two.csv###path/three.csv",
                ]
            ),
        )

    def test_requires_normalization_files_excluded(self) -> None:
        labels = labels_from_arguments(
            [
                "--entrypoint=RawDataFileChunkingEntrypoint",
                "--state_code=US_TN",
                "--requires_normalization_files=a.csv###b.csv",
            ]
        )
        self.assertNotIn("recidiviz.org/requires_normalization_files", labels)
        self.assertNotIn("recidiviz.org/file_chunks", labels)
        self.assertEqual(
            {
                "recidiviz.org/entrypoint": "RawDataFileChunkingEntrypoint",
                "recidiviz.org/state_code": "US_TN",
            },
            labels,
        )

    def test_non_flag_args_ignored(self) -> None:
        self.assertEqual(
            {},
            labels_from_arguments(["run", "--no-sync", "python", "-m", "some.module"]),
        )

    def test_real_values_pass_through_sanitizer_unchanged(self) -> None:
        # Our allowlisted values (entrypoint class names, state codes) are
        # already valid, short label values, so make_safe_label_value is a no-op.
        self.assertEqual(
            {
                "recidiviz.org/entrypoint": "RawDataChunkNormalizationEntrypoint",
                "recidiviz.org/state_code": "US_MI",
            },
            labels_from_arguments(
                [
                    "--entrypoint=RawDataChunkNormalizationEntrypoint",
                    "--state_code=US_MI",
                ]
            ),
        )

    def test_over_long_value_is_truncated_to_valid_label(self) -> None:
        # Defers to Airflow's make_safe_label_value, which keeps values <= 63
        # chars (truncating + hashing past the limit). We don't expect this for
        # real values; just confirm we never emit an invalid label.
        labels = labels_from_arguments(["--entrypoint=" + "A" * 100])
        value = labels["recidiviz.org/entrypoint"]
        self.assertLessEqual(len(value), 63)
        self.assertTrue(value.startswith("A"))

    def test_empty_value_is_dropped(self) -> None:
        self.assertEqual({}, labels_from_arguments(["--state_code="]))

    def test_all_fixture_entrypoints_get_entrypoint_label(self) -> None:
        # Every known entrypoint usage carries an --entrypoint arg, so each must
        # produce the entrypoint label.
        for name, argv in self.entrypoint_args_fixture.items():
            with self.subTest(entrypoint_usage=name):
                labels = labels_from_arguments(argv)
                self.assertIn(f"{RECIDIVIZ_LABEL_PREFIX}entrypoint", labels)


@patch(
    "recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator.get_project_id",
    return_value=None,
)
class TestBuildPodRequestObjLabels(unittest.TestCase):
    """Tests that identity labels are applied to the built pod object.

    These guard the regression where labels assigned to self.labels in execute()
    did not land on dynamically-mapped pods. Applying them in
    build_pod_request_obj (where KubernetesPodOperator applies its own
    dag_id/task_id/map_index labels) fixes that, so we assert against the pod the
    operator actually builds — not just labels_from_arguments in isolation.
    """

    def _build_pod_labels(self, arguments: list[str]) -> dict[str, str]:
        operator = RecidivizKubernetesPodOperator(
            task_id="raw_data_chunk_normalization",
            arguments=arguments,
        )
        # Stand in for the pod KubernetesPodOperator builds, including the
        # task-identifying labels it always stamps on (which we have confirmed do
        # reach mapped pods); our override must merge the entrypoint labels on top.
        base_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="recidiviz-kubernetes-pod",
                labels={"dag_id": "raw_data_import_dag", "map_index": "3"},
            ),
            status=k8s.V1PodStatus(
                phase="RUNNING",
                start_time=None,
            ),
        )
        with patch.object(
            RecidivizKubernetesPodOperator.__bases__[0],
            "build_pod_request_obj",
            return_value=base_pod,
        ):
            pod = operator.build_pod_request_obj()
        labels = pod.metadata.labels
        assert labels is not None
        return labels

    def test_entrypoint_and_state_labels_applied_to_pod(self, _: Any) -> None:
        labels = self._build_pod_labels(
            [
                "run",
                "--entrypoint=RawDataChunkNormalizationEntrypoint",
                "--state_code=US_MI",
                "--file_chunks=path/one.csv###path/two.csv",
            ]
        )
        self.assertEqual(
            "RawDataChunkNormalizationEntrypoint",
            labels["recidiviz.org/entrypoint"],
        )
        self.assertEqual("US_MI", labels["recidiviz.org/state_code"])

    def test_pod_identifying_labels_preserved(self, _: Any) -> None:
        # Merging the entrypoint labels must not clobber the labels
        # KubernetesPodOperator already applied to the pod.
        labels = self._build_pod_labels(
            ["--entrypoint=RawDataChunkNormalizationEntrypoint"]
        )
        self.assertEqual("raw_data_import_dag", labels["dag_id"])
        self.assertEqual("3", labels["map_index"])
        self.assertIn("recidiviz.org/entrypoint", labels)
