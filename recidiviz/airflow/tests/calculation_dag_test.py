# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
from airflow.providers.google.cloud.operators.tasks import CloudTasksTaskCreateOperator
from more_itertools import one

from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowTemplateOperator,
)
from recidiviz.airflow.tests.test_utils import AIRFLOW_WORKING_DIRECTORY, DAG_FOLDER
from recidiviz.calculator import pipeline

_PROJECT_ID = "recidiviz-testing"
CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH = os.path.join(
    os.path.relpath(
        os.path.dirname(pipeline.__file__),
        start=AIRFLOW_WORKING_DIRECTORY,
    ),
    "calculation_pipeline_templates.yaml",
)

_TRIGGER_REMATERIALIZATION_TASK_ID = "trigger_rematerialize_views_task"
_WAIT_FOR_REMATERIALIZATION_TASK_ID = "wait_for_view_rematerialization_success"
_UPDATE_ALL_VIEWS_BRANCH_TASK_ID = "update_all_views_branch"
_TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID = "trigger_update_all_managed_views_task"
_WAIT_FOR_UPDATE_ALL_MANAGED_VIEWS_TASK_ID = "wait_for_view_update_all_success"
_TRIGGER_VALIDATIONS_TASK_ID_REGEX = r"trigger_us_[a-z]{2}_validations_task"
_WAIT_FOR_VALIDATIONS_TASK_ID_REGEX = r"wait_for_(us_[a-z]{2})_validations_completion"
_ACQUIRE_LOCK_TASK_ID = "acquire_lock_STATE"
_WAIT_FOR_CAN_REFRESH_PROCEED_TASK_ID = "wait_for_acquire_lock_success_STATE"
_TRIGGER_REFRESH_BQ_DATASET_TASK_ID = "trigger_refresh_bq_dataset_task_STATE"
_WAIT_FOR_REFRESH_BQ_DATASET_SUCCESS_ID = "wait_for_refresh_bq_dataset_success_STATE"


def get_post_refresh_short_circuit_task_id(schema_type: str) -> str:
    return f"post_refresh_short_circuit_{schema_type.upper()}"


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

    def test_update_normalized_state_upstream_of_rematerialization(self) -> None:
        """Tests that the `normalized_state` dataset update happens before views
        are rematerialized (and therefore before metric export, where relevant)."""
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
            _TRIGGER_REMATERIALIZATION_TASK_ID,
            normalized_state_downstream_dag.task_ids,
        )

    def test_update_normalized_state_upstream_of_normalization_pipelines(self) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        normalization pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        normalization_pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowTemplateOperator)
            and "normalization" in task.task_id
        }

        normalized_state_upstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=False,
            include_upstream=True,
        )
        self.assertNotEqual(0, len(normalized_state_upstream_dag.task_ids))

        upstream_tasks = set()
        for task in normalized_state_upstream_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = normalization_pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_rematerialization_upstream_of_all_exports(
        self,
    ) -> None:
        """Tests that view rematerialization happens before any of the metric exports."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_view_data_export_id_regex = "trigger.*metric_view_data_export.*"
        metric_view_data_export_tasks = dag.partial_subset(
            task_ids_or_regex=metric_view_data_export_id_regex,
            include_downstream=False,
            include_upstream=True,
        )

        self.assertNotEqual(0, len(metric_view_data_export_tasks.leaves))
        for task in metric_view_data_export_tasks.leaves:
            self.assertRegex(task.task_id, metric_view_data_export_id_regex)
            self.assertIn(_WAIT_FOR_REMATERIALIZATION_TASK_ID, task.upstream_task_ids)

    def test_rematerialization_upstream_of_validation(
        self,
    ) -> None:
        """Tests that view rematerialization happens before the validations run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_for_materialization_downstream_dag = dag.partial_subset(
            task_ids_or_regex=[_WAIT_FOR_REMATERIALIZATION_TASK_ID],
            include_downstream=True,
            include_upstream=False,
        )

        self.assertNotEqual(0, len(wait_for_materialization_downstream_dag.task_ids))

        found_downstream = False
        for upstream_task_id in wait_for_materialization_downstream_dag.task_ids:
            if re.match(_TRIGGER_VALIDATIONS_TASK_ID_REGEX, upstream_task_id):
                found_downstream = True

        self.assertTrue(found_downstream)

        wait_for_materialization_upstream_dag = dag.partial_subset(
            task_ids_or_regex=[_WAIT_FOR_REMATERIALIZATION_TASK_ID],
            include_downstream=False,
            include_upstream=True,
        )

        found_upstream = False
        for upstream_task_id in wait_for_materialization_upstream_dag.task_ids:
            if re.match(_TRIGGER_VALIDATIONS_TASK_ID_REGEX, upstream_task_id):
                found_upstream = True

        self.assertFalse(found_upstream)

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
            include_upstream=True,
        )
        self.assertNotEqual(0, len(wait_subdag.leaves))
        for wait_task in wait_subdag.leaves:
            match = re.match(_WAIT_FOR_VALIDATIONS_TASK_ID_REGEX, wait_task.task_id)
            if not match:
                raise ValueError(f"Found unexpected leaf node: {wait_task.task_id}")
            self.assertEqual(
                {f"trigger_{match.group(1)}_validations_task"},
                wait_task.upstream_task_ids,
            )

    def test_rematerialization_downstream_of_all_pipelines(
        self,
    ) -> None:
        """Tests that view rematerialization happens after all pipelines have run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowTemplateOperator)
        }

        self.assertNotEqual(0, len(pipeline_task_ids))

        trigger_task_subdag = dag.partial_subset(
            task_ids_or_regex=_TRIGGER_REMATERIALIZATION_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )

        upstream_tasks = set()
        for task in trigger_task_subdag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_trigger_rematerialization_upstream_of_wait(self) -> None:
        """Tests that view rematerialization trigger happens directly before we wait
        for the materialization to finish.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_WAIT_FOR_REMATERIALIZATION_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_WAIT_FOR_REMATERIALIZATION_TASK_ID, wait_task.task_id)
        self.assertEqual(
            {_TRIGGER_REMATERIALIZATION_TASK_ID}, wait_task.upstream_task_ids
        )

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

    def test_trigger_refresh_bq_dataset_task_upstream_of_wait(self) -> None:
        """Tests that trigger_refresh_bq_dataset_task trigger happens directly before we wait
        for the refresh dataset endpoint to finish.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_WAIT_FOR_REFRESH_BQ_DATASET_SUCCESS_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_WAIT_FOR_REFRESH_BQ_DATASET_SUCCESS_ID, wait_task.task_id)
        self.assertEqual(
            {_TRIGGER_REFRESH_BQ_DATASET_TASK_ID},
            wait_task.upstream_task_ids,
        )

    def test_acquire_lock_task_upstream_of_wait_for_acquire_lock_success(self) -> None:
        """Tests that acquire_lock happens directly before we call wait_for_acquire_lock_success."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_WAIT_FOR_CAN_REFRESH_PROCEED_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_WAIT_FOR_CAN_REFRESH_PROCEED_TASK_ID, wait_task.task_id)
        self.assertEqual(
            {_ACQUIRE_LOCK_TASK_ID},
            wait_task.upstream_task_ids,
        )

    def test_wait_for_acquire_lock_success_task_upstream_of_trigger_refresh_bq_dataset(
        self,
    ) -> None:
        """Tests that wait_for_acquire_lock_success happens directly before we call trigger_refresh_bq_dataset."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_TRIGGER_REFRESH_BQ_DATASET_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_TRIGGER_REFRESH_BQ_DATASET_TASK_ID, wait_task.task_id)
        self.assertEqual(
            {_WAIT_FOR_CAN_REFRESH_PROCEED_TASK_ID},
            wait_task.upstream_task_ids,
        )

    def test_wait_for_refresh_bq_dataset_task_upstream_of_state_bq_refresh_completion(
        self,
    ) -> None:
        """Tests that wait_for_refresh_bq_dataset happens directly before we call state_bq_refresh_completion."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=get_post_refresh_short_circuit_task_id("STATE"),
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(
            get_post_refresh_short_circuit_task_id("STATE"), wait_task.task_id
        )
        self.assertEqual(
            {_WAIT_FOR_REFRESH_BQ_DATASET_SUCCESS_ID},
            wait_task.upstream_task_ids,
        )

    def test_state_bq_refresh_completion_task_upstream_of_update_all_views_branch(
        self,
    ) -> None:
        """Tests that state_bq_refresh_completion happens directly before we call update_all_views_branch."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_UPDATE_ALL_VIEWS_BRANCH_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_UPDATE_ALL_VIEWS_BRANCH_TASK_ID, wait_task.task_id)
        self.assertEqual(
            {
                get_post_refresh_short_circuit_task_id("STATE"),
                get_post_refresh_short_circuit_task_id("OPERATIONS"),
                get_post_refresh_short_circuit_task_id("CASE_TRIAGE"),
            },
            wait_task.upstream_task_ids,
        )

    def test_update_all_views_branch_task_upstream_of_update_all_managed_views(
        self,
    ) -> None:
        """Tests that update_all_views_branch happens directly before we call update_all_managed_views."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        wait_subdag = dag.partial_subset(
            task_ids_or_regex=_TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )
        wait_task = one(wait_subdag.leaves)

        self.assertEqual(_TRIGGER_UPDATE_ALL_MANAGED_VIEWS_TASK_ID, wait_task.task_id)
        self.assertEqual(
            {
                _UPDATE_ALL_VIEWS_BRANCH_TASK_ID,
            },
            wait_task.upstream_task_ids,
        )

    def test_rematerialization_endpoint(self) -> None:
        """Tests that rematerialization triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_cloud_task_task = dag.get_task(_TRIGGER_REMATERIALIZATION_TASK_ID)

        if not isinstance(trigger_cloud_task_task, CloudTasksTaskCreateOperator):
            raise ValueError(
                f"Expected type CloudTasksTaskCreateOperator, found "
                f"[{type(trigger_cloud_task_task)}]."
            )

        self.assertEqual("bq-view-update", trigger_cloud_task_task.queue_name)

        self.assertEqual(
            trigger_cloud_task_task.task.app_engine_http_request.relative_uri,
            "/view_update/rematerialize_all_deployed_views",
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

    def test_trigger_refresh_bq_dataset_task_endpoint(self) -> None:
        """Tests that trigger_refresh_bq_dataset_task triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_cloud_task_task = dag.get_task(_TRIGGER_REFRESH_BQ_DATASET_TASK_ID)

        if not isinstance(trigger_cloud_task_task, CloudTasksTaskCreateOperator):
            raise ValueError(
                f"Expected type CloudTasksTaskCreateOperator, found "
                f"[{type(trigger_cloud_task_task)}]."
            )

        self.assertEqual("bq-view-update", trigger_cloud_task_task.queue_name)

        self.assertEqual(
            trigger_cloud_task_task.task.app_engine_http_request.relative_uri,
            "/cloud_sql_to_bq/refresh_bq_dataset/STATE",
        )

    def test_validations_endpoint(self) -> None:
        """Tests that validation triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_cloud_task_task = dag.get_task("trigger_us_nd_validations_task")

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
