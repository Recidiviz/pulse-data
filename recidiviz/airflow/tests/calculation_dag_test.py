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
"""
Unit test to test the calculation pipeline DAG logic.
"""
import os
import unittest
from typing import Set
from unittest import mock
from unittest.mock import MagicMock, patch

from airflow.models.baseoperator import BaseOperator
from airflow.models.dagbag import DagBag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.task_group import TaskGroup

from recidiviz import pipelines
from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.tests.test_utils import AIRFLOW_WORKING_DIRECTORY, DAG_FOLDER

# Need to import calculation_dag inside test suite so environment variables are set before importing,
# otherwise calculation_dag will raise an Error and not import.
# pylint: disable=C0415 import-outside-toplevel

_PROJECT_ID = "recidiviz-testing"
CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH = os.path.join(
    os.path.relpath(
        os.path.dirname(pipelines.__file__),
        start=AIRFLOW_WORKING_DIRECTORY,
    ),
    "calculation_pipeline_templates.yaml",
)

_UPDATE_ALL_MANAGED_VIEWS_TASK_ID = "update_all_managed_views"
_VALIDATIONS_STATE_CODE_BRANCH_START = "validations.state_code_branch_start"
_REFRESH_BQ_DATASET_TASK_ID = "bq_refresh.refresh_bq_dataset_STATE"
_EXPORT_METRIC_VIEW_DATA_TASK_ID = "metric_exports.INGEST_METADATA_metric_exports.export_ingest_metadata_metric_view_data"


def get_post_refresh_release_lock_task_id(schema_type: str) -> str:
    return f"bq_refresh.{schema_type.lower()}_bq_refresh.release_lock_{schema_type.upper()}"


class TestCalculationPipelineDag(unittest.TestCase):
    """Tests the calculation pipeline DAGs."""

    CALCULATION_DAG_ID = f"{_PROJECT_ID}_calculation_dag"

    def setUp(self) -> None:
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
                "CONFIG_FILE": CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
            },
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()

    def test_bq_refresh_downstream_initialize_dag(self) -> None:
        """Tests that the `bq_refresh` task is downstream of initialize_dag."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        bq_refresh_group: TaskGroup = dag.task_group_dict["bq_refresh"]
        initialize_dag = dag.task_group_dict["initialize_dag"]

        self.assertIn(
            initialize_dag.group_id,
            bq_refresh_group.upstream_group_ids,
        )

    def test_update_normalized_state_upstream_of_view_update(self) -> None:
        """Tests that the `normalized_state` dataset update happens before views
        are updated (and therefore before metric export, where relevant)."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        normalized_state_downstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=True,
            include_upstream=False,
        )

        self.assertNotEqual(0, len(normalized_state_downstream_dag.task_ids))

        self.assertIn(
            _UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            normalized_state_downstream_dag.task_ids,
        )

    def test_update_normalized_state_downstream_of_normalization_pipelines(
        self,
    ) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        normalization pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        normalization_group: TaskGroup = dag.task_group_dict["normalization"]
        update_normalized_state_task = dag.get_task("update_normalized_state")

        self.assertIn(
            update_normalized_state_task.task_id,
            normalization_group.downstream_task_ids,
        )

    def test_update_normalized_state_upstream_of_non_normalization_pipelines(
        self,
    ) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        normalization pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowFlexTemplateOperator)
            and "normalization" not in task.task_id
        }

        normalized_state_upstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=True,
            include_upstream=False,
        )
        self.assertNotEqual(0, len(normalized_state_upstream_dag.task_ids))

        upstream_tasks = set()
        for task in normalized_state_upstream_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = metric_pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_update_normalized_state_upstream_of_non_normalization_flex_pipelines(
        self,
    ) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        non normalization flex pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowFlexTemplateOperator)
            and "normalization" not in task.task_id
        }

        normalized_state_upstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=True,
            include_upstream=False,
        )
        self.assertNotEqual(0, len(normalized_state_upstream_dag.task_ids))

        upstream_tasks = set()
        for task in normalized_state_upstream_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = metric_pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_update_all_views_branch_upstream_of_all_exports(
        self,
    ) -> None:
        """Tests that view update happens before any of the metric exports."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_exports_group: TaskGroup = dag.task_group_dict["metric_exports"]
        view_materialization: BaseOperator = dag.get_task(
            _UPDATE_ALL_MANAGED_VIEWS_TASK_ID
        )
        self.assertIn(
            view_materialization.task_id, metric_exports_group.upstream_task_ids
        )

    def test_update_all_views_upstream_of_validation(
        self,
    ) -> None:
        """Tests that update_all_views happens before the validations run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        validations_start: BaseOperator = dag.get_task(
            _VALIDATIONS_STATE_CODE_BRANCH_START
        )
        view_materialization: BaseOperator = dag.get_task(
            _UPDATE_ALL_MANAGED_VIEWS_TASK_ID
        )

        self.assertIn(
            validations_start.task_id, view_materialization.downstream_task_ids
        )
        self.assertNotIn(
            validations_start.task_id, view_materialization.upstream_task_ids
        )

    def test_view_update_downstream_of_all_pipelines(
        self,
    ) -> None:
        """Tests that view update happens after all pipelines have run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowFlexTemplateOperator)
        }

        self.assertNotEqual(0, len(pipeline_task_ids))

        trigger_task_subdag = dag.partial_subset(
            task_ids_or_regex=_UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )

        upstream_tasks = set()
        for task in trigger_task_subdag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_update_all_managed_views_endpoint_exists(self) -> None:
        """Tests that update_all_managed_views triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_update_task = dag.get_task(_UPDATE_ALL_MANAGED_VIEWS_TASK_ID)

        if not isinstance(trigger_update_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_update_task)}]."
            )

    @patch(
        "recidiviz.airflow.dags.calculation_dag.build_recidiviz_kubernetes_pod_operator"
    )
    def test_update_all_managed_views_endpoint(
        self, mock_build_kubernetes_pod_creator: MagicMock
    ) -> None:
        from recidiviz.airflow.dags.calculation_dag import (
            update_all_managed_views_operator,
        )

        update_all_managed_views_operator()

        mock_build_kubernetes_pod_creator.assert_called_once_with(
            task_id="update_all_managed_views",
            container_name="update_all_managed_views",
            arguments=[
                "python",
                "-m",
                "recidiviz.entrypoints.view_update.update_all_managed_views",
                f"--project_id={_PROJECT_ID}",
            ],
        )

    def test_refresh_bq_dataset_task_exists(self) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        bq_refresh_task = dag.get_task(_REFRESH_BQ_DATASET_TASK_ID)

        if not isinstance(bq_refresh_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(bq_refresh_task)}]."
            )

    @patch("recidiviz.airflow.dags.calculation_dag.get_ingest_instance")
    @patch("recidiviz.airflow.dags.calculation_dag.get_sandbox_prefix")
    @patch(
        "recidiviz.airflow.dags.calculation_dag.build_recidiviz_kubernetes_pod_operator"
    )
    def test_refresh_bq_dataset_task(
        self,
        mock_build_kubernetes_pod_creator: MagicMock,
        mock_get_sandbox_prefix: MagicMock,
        mock_get_ingest_instance: MagicMock,
    ) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import refresh_bq_dataset_operator

        mock_get_sandbox_prefix.return_value = None
        mock_get_ingest_instance.return_value = "PRIMARY"

        refresh_bq_dataset_operator("STATE")

        mock_build_kubernetes_pod_creator.assert_called_once_with(
            task_id="refresh_bq_dataset_STATE",
            container_name="refresh_bq_dataset_STATE",
            arguments=mock.ANY,
        )

        arguments = mock_build_kubernetes_pod_creator.mock_calls[0].kwargs["arguments"]
        if not callable(arguments):
            raise ValueError(f"Expected callable arguments, found [{type(arguments)}].")

        self.assertEqual(
            arguments(MagicMock(), MagicMock()),
            [
                "python",
                "-m",
                "recidiviz.entrypoints.bq_refresh.cloud_sql_to_bq_refresh",
                f"--project_id={_PROJECT_ID}",
                "--schema_type=STATE",
                "--ingest_instance=PRIMARY",
            ],
        )

    @patch("recidiviz.airflow.dags.calculation_dag.get_ingest_instance")
    @patch("recidiviz.airflow.dags.calculation_dag.get_sandbox_prefix")
    @patch(
        "recidiviz.airflow.dags.calculation_dag.build_recidiviz_kubernetes_pod_operator"
    )
    def test_refresh_bq_dataset_task_secondary(
        self,
        mock_build_kubernetes_pod_creator: MagicMock,
        mock_get_sandbox_prefix: MagicMock,
        mock_get_ingest_instance: MagicMock,
    ) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import refresh_bq_dataset_operator

        mock_get_sandbox_prefix.return_value = "test_prefix"
        mock_get_ingest_instance.return_value = "SECONDARY"

        refresh_bq_dataset_operator("STATE")

        mock_build_kubernetes_pod_creator.assert_called_once_with(
            task_id="refresh_bq_dataset_STATE",
            container_name="refresh_bq_dataset_STATE",
            arguments=mock.ANY,
        )

        arguments = mock_build_kubernetes_pod_creator.mock_calls[0].kwargs["arguments"]
        if not callable(arguments):
            raise ValueError(f"Expected callable arguments, found [{type(arguments)}].")

        self.assertEqual(
            arguments(MagicMock(), MagicMock()),
            [
                "python",
                "-m",
                "recidiviz.entrypoints.bq_refresh.cloud_sql_to_bq_refresh",
                f"--project_id={_PROJECT_ID}",
                "--schema_type=STATE",
                "--ingest_instance=SECONDARY",
                "--sandbox_prefix=test_prefix",
            ],
        )

    def test_validations_task_exists(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_validation_task = dag.get_task("validations.execute_validations_US_ND")

        if not isinstance(trigger_validation_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_validation_task)}]."
            )

    @patch("recidiviz.airflow.dags.calculation_dag.get_ingest_instance")
    @patch("recidiviz.airflow.dags.calculation_dag.get_sandbox_prefix")
    @patch(
        "recidiviz.airflow.dags.calculation_dag.build_recidiviz_kubernetes_pod_operator"
    )
    def test_validations_task(
        self,
        mock_build_kubernetes_pod_creator: MagicMock,
        mock_get_sandbox_prefix: MagicMock,
        mock_get_ingest_instance: MagicMock,
    ) -> None:
        """Tests that validation triggers the proper endpoint."""
        from recidiviz.airflow.dags.calculation_dag import execute_validations_operator

        mock_get_sandbox_prefix.return_value = None
        mock_get_ingest_instance.return_value = "PRIMARY"

        execute_validations_operator(state_code="US_ND")

        mock_build_kubernetes_pod_creator.assert_called_once_with(
            task_id="execute_validations_US_ND",
            container_name="execute_validations_US_ND",
            arguments=mock.ANY,
        )

        arguments = mock_build_kubernetes_pod_creator.mock_calls[0].kwargs["arguments"]
        if not callable(arguments):
            raise ValueError(f"Expected callable arguments, found [{type(arguments)}].")

        self.assertEqual(
            arguments(MagicMock(), MagicMock()),
            [
                "python",
                "-m",
                "recidiviz.entrypoints.validation.validate",
                f"--project_id={_PROJECT_ID}",
                "--state_code=US_ND",
                "--ingest_instance=PRIMARY",
            ],
        )

    @patch("recidiviz.airflow.dags.calculation_dag.get_ingest_instance")
    @patch("recidiviz.airflow.dags.calculation_dag.get_sandbox_prefix")
    @patch(
        "recidiviz.airflow.dags.calculation_dag.build_recidiviz_kubernetes_pod_operator"
    )
    def test_validations_task_secondary(
        self,
        mock_build_kubernetes_pod_creator: MagicMock,
        mock_get_sandbox_prefix: MagicMock,
        mock_get_ingest_instance: MagicMock,
    ) -> None:
        """Tests that validation triggers the proper endpoint."""
        from recidiviz.airflow.dags.calculation_dag import execute_validations_operator

        mock_get_sandbox_prefix.return_value = "test_prefix"
        mock_get_ingest_instance.return_value = "SECONDARY"

        execute_validations_operator(state_code="US_ND")

        mock_build_kubernetes_pod_creator.assert_called_once_with(
            task_id="execute_validations_US_ND",
            container_name="execute_validations_US_ND",
            arguments=mock.ANY,
        )

        arguments = mock_build_kubernetes_pod_creator.mock_calls[0].kwargs["arguments"]
        if not callable(arguments):
            raise ValueError(f"Expected callable arguments, found [{type(arguments)}].")

        self.assertEqual(
            arguments(MagicMock(), MagicMock()),
            [
                "python",
                "-m",
                "recidiviz.entrypoints.validation.validate",
                f"--project_id={_PROJECT_ID}",
                "--state_code=US_ND",
                "--ingest_instance=SECONDARY",
                "--sandbox_prefix=test_prefix",
            ],
        )

    def test_trigger_metric_view_data_operator_exists(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]

        trigger_metric_view_data_task = dag.get_task(_EXPORT_METRIC_VIEW_DATA_TASK_ID)
        if not isinstance(trigger_metric_view_data_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_metric_view_data_task)}]."
            )

    @patch(
        "recidiviz.airflow.dags.calculation_dag.build_recidiviz_kubernetes_pod_operator"
    )
    def test_trigger_metric_view_data_operator(
        self, mock_build_kubernetes_pod_creator: MagicMock
    ) -> None:
        """Tests the trigger_metric_view_data_operator triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import (
            trigger_metric_view_data_operator,
        )

        trigger_metric_view_data_operator(
            export_job_name="INGEST_METADATA", state_code=None
        )

        mock_build_kubernetes_pod_creator.assert_called_once_with(
            task_id="export_ingest_metadata_metric_view_data",
            container_name="export_ingest_metadata_metric_view_data",
            arguments=[
                "python",
                "-m",
                "recidiviz.entrypoints.metric_export.metric_view_export",
                f"--project_id={_PROJECT_ID}",
                "--export_job_name=INGEST_METADATA",
            ],
        )
