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
import re
import unittest
from typing import Set
from unittest.mock import patch

from airflow.models.dagbag import DagBag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.providers.google.cloud.operators.tasks import CloudTasksTaskCreateOperator
from airflow.utils.task_group import TaskGroup
from more_itertools import one

from recidiviz import pipelines
from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.tests.test_utils import AIRFLOW_WORKING_DIRECTORY, DAG_FOLDER

_PROJECT_ID = "recidiviz-testing"
CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH = os.path.join(
    os.path.relpath(
        os.path.dirname(pipelines.__file__),
        start=AIRFLOW_WORKING_DIRECTORY,
    ),
    "calculation_pipeline_templates.yaml",
)

_TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID = (
    "view_materialization.trigger_update_all_managed_views_task"
)
_WAIT_FOR_UPDATE_ALL_MANAGED_VIEWS_TASK_ID = (
    "view_materialization.wait_for_view_update_all_success"
)
_WAIT_FOR_VALIDATIONS_TASK_ID_REGEX = (
    r"validations.(US_[A-Z]{2})_validations.wait_for_validations_completion"
)
_TRIGGER_REFRESH_BQ_DATASET_TASK_ID = "bq_refresh.trigger_refresh_bq_dataset_task_STATE"
_VIEW_MATERIALIZATION_GROUP_ID = "view_materialization"


def get_post_refresh_release_lock_task_id(schema_type: str) -> str:
    return f"bq_refresh.{schema_type.lower()}_bq_refresh.release_lock_{schema_type.upper()}"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
        "CONFIG_FILE": CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
    },
)
class TestCalculationPipelineDag(unittest.TestCase):
    """Tests the calculation pipeline DAGs."""

    CALCULATION_DAG_ID = f"{_PROJECT_ID}_calculation_dag"

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
            _TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
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
        view_materialization_group: TaskGroup = dag.task_group_dict[
            _VIEW_MATERIALIZATION_GROUP_ID
        ]
        self.assertIn(
            view_materialization_group.group_id, metric_exports_group.upstream_group_ids
        )

    def test_update_all_views_upstream_of_validation(
        self,
    ) -> None:
        """Tests that update_all_views happens before the validations run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        validations_group: TaskGroup = dag.task_group_dict["validations"]
        view_materialization_group: TaskGroup = dag.task_group_dict[
            _VIEW_MATERIALIZATION_GROUP_ID
        ]

        self.assertIn(
            validations_group.group_id, view_materialization_group.downstream_group_ids
        )
        self.assertNotIn(
            validations_group.group_id, view_materialization_group.upstream_group_ids
        )

    def test_trigger_validations_upstream_of_wait(self) -> None:
        """Tests that the validations trigger happens directly before we wait
        for the validations to finish.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_WAIT_FOR_VALIDATIONS_TASK_ID_REGEX,
            include_downstream=False,
            include_upstream=False,
        )
        self.assertNotEqual(0, len(wait_subdag.tasks))
        for wait_task in wait_subdag.tasks:
            match = re.match(_WAIT_FOR_VALIDATIONS_TASK_ID_REGEX, wait_task.task_id)
            if not match:
                raise ValueError(f"Found unexpected node: {wait_task.task_id}")
            trigger_task = dag.get_task(
                f"validations.{match.group(1).upper()}_validations.trigger_validations_task"
            )
            self.assertEqual(
                trigger_task.downstream_task_ids,
                {wait_task.task_id},
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
            task_ids_or_regex=_TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )

        upstream_tasks = set()
        for task in trigger_task_subdag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_trigger_update_all_managed_views_upstream_of_wait(self) -> None:
        """Tests that update_all_managed_views trigger happens directly before we wait
        for the update all views endpoint to finish.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_WAIT_FOR_UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_WAIT_FOR_UPDATE_ALL_MANAGED_VIEWS_TASK_ID, wait_task.task_id)
        self.assertEqual(
            {_TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID},
            wait_task.upstream_task_ids,
        )

    def test_update_all_managed_views_endpoint(self) -> None:
        """Tests that update_all_managed_views triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_cloud_task_task = dag.get_task(
            _TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID
        )

        if not isinstance(trigger_cloud_task_task, CloudTasksTaskCreateOperator):
            raise ValueError(
                f"Expected type CloudTasksTaskCreateOperator, found "
                f"[{type(trigger_cloud_task_task)}]."
            )

        self.assertEqual("bq-view-update", trigger_cloud_task_task.queue_name)

        self.assertEqual(
            trigger_cloud_task_task.task.app_engine_http_request.relative_uri,
            "/view_update/update_all_managed_views",
        )

    def test_trigger_refresh_bq_dataset_task(self) -> None:
        """Tests that trigger_refresh_bq_dataset_task triggers the proper script."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_bq_refresh_task = dag.get_task(_TRIGGER_REFRESH_BQ_DATASET_TASK_ID)

        if not isinstance(trigger_bq_refresh_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_bq_refresh_task)}]."
            )

        self.assertEqual(
            trigger_bq_refresh_task.arguments,
            [
                "run",
                "python",
                "-m",
                "recidiviz.entrypoints.bq_refresh.cloud_sql_to_bq_refresh",
                f"--project_id={_PROJECT_ID}",
                "--schema_type=STATE",
                "--ingest_instance=PRIMARY",
            ],
        )

    def test_validations_endpoint(self) -> None:
        """Tests that validation triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_cloud_task_task = dag.get_task(
            "validations.US_ND_validations.trigger_validations_task"
        )

        if not isinstance(trigger_cloud_task_task, CloudTasksTaskCreateOperator):
            raise ValueError(
                f"Expected type CloudTasksTaskCreateOperator, found "
                f"[{type(trigger_cloud_task_task)}]."
            )

        self.assertEqual("validations", trigger_cloud_task_task.queue_name)

        self.assertEqual(
            trigger_cloud_task_task.task.app_engine_http_request.relative_uri,
            "/validation_manager/validate/US_ND",
        )
