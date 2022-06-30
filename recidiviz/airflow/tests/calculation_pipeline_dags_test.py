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
import unittest
from typing import Set
from unittest.mock import patch

from airflow.models.dagbag import DagBag
from airflow.providers.google.cloud.operators.tasks import CloudTasksTaskCreateOperator
from more_itertools import one

from recidiviz.airflow.dags.operators.iap_httprequest_operator import (
    IAPHTTPRequestOperator,
)
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
_RUN_VALIDATIONS_TASK_ID = "run_all_validations"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
        "CONFIG_FILE": CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
    },
)
class TestCalculationPipelineDags(unittest.TestCase):
    """Tests the calculation pipeline DAGs."""

    INCREMENTAL_DAG_ID = f"{_PROJECT_ID}_incremental_calculation_pipeline_dag"
    HISTORICAL_DAG_ID = f"{_PROJECT_ID}_historical_calculation_pipeline_dag"

    def setUp(self) -> None:
        self.calc_pipeline_dag_ids = [
            self.INCREMENTAL_DAG_ID,
            self.HISTORICAL_DAG_ID,
        ]

    def test_update_normalized_state_upstream_of_rematerialization(self) -> None:
        """Tests that the `normalized_state` dataset update happens before views
        are rematerialized (and therefore before metric export, where relevant)."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)

        for dag_id in self.calc_pipeline_dag_ids:
            dag = dag_bag.dags[dag_id]
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

        for dag_id in self.calc_pipeline_dag_ids:
            dag = dag_bag.dags[dag_id]
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

            pipeline_tasks_not_upstream = (
                normalization_pipeline_task_ids - upstream_tasks
            )
            self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_rematerialization_upstream_of_all_exports(
        self,
    ) -> None:
        """Tests that view rematerialization happens before any of the metric exports."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        for dag_id in self.calc_pipeline_dag_ids:
            # TODO(#9010): Remove this once the historical and incremental DAGs have
            #  the same structure and historical triggers exports.
            if dag_id == self.HISTORICAL_DAG_ID:
                continue
            dag = dag_bag.dags[dag_id]
            self.assertNotEqual(0, len(dag.task_ids))

            export_task_id_regex = ".*bq_metric_export.*"
            export_tasks = dag.partial_subset(
                task_ids_or_regex=export_task_id_regex,
                include_downstream=False,
                include_upstream=True,
            )

            self.assertNotEqual(0, len(export_tasks.leaves))
            for task in export_tasks.leaves:
                self.assertRegex(task.task_id, export_task_id_regex)
                self.assertIn(
                    _WAIT_FOR_REMATERIALIZATION_TASK_ID, task.upstream_task_ids
                )

    def test_rematerialization_upstream_of_validation(
        self,
    ) -> None:
        """Tests that view rematerialization happens before the validations run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        for dag_id in self.calc_pipeline_dag_ids:
            # TODO(#9010): Remove this once the historical and incremental DAGs have
            #  the same structure and historical triggers exports.
            if dag_id == self.HISTORICAL_DAG_ID:
                continue
            dag = dag_bag.dags[dag_id]
            self.assertNotEqual(0, len(dag.task_ids))

            wait_for_materialization_downstream_dag = dag.partial_subset(
                task_ids_or_regex=[_WAIT_FOR_REMATERIALIZATION_TASK_ID],
                include_downstream=True,
                include_upstream=False,
            )

            self.assertNotEqual(
                0, len(wait_for_materialization_downstream_dag.task_ids)
            )

            self.assertIn(
                _RUN_VALIDATIONS_TASK_ID,
                wait_for_materialization_downstream_dag.task_ids,
            )

    def test_rematerialization_downstream_of_all_pipelines(
        self,
    ) -> None:
        """Tests that view rematerialization happens after all pipelines have run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        for dag_id in self.calc_pipeline_dag_ids:
            dag = dag_bag.dags[dag_id]
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
        for dag_id in self.calc_pipeline_dag_ids:
            dag = dag_bag.dags[dag_id]
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

    def test_rematerialization_endpoint(self) -> None:
        """Tests that rematerialization triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        for dag_id in self.calc_pipeline_dag_ids:
            dag = dag_bag.dags[dag_id]
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

    def test_validations_endpoint(self) -> None:
        """Tests that the validations task triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        for dag_id in self.calc_pipeline_dag_ids:
            dag = dag_bag.dags[dag_id]
            validations_task = dag.get_task(_RUN_VALIDATIONS_TASK_ID)

            if not isinstance(validations_task, IAPHTTPRequestOperator):
                raise ValueError(
                    f"Expected type IAPHTTPRequestOperator, found "
                    f"[{type(validations_task)}]."
                )

            self.assertEqual(
                validations_task.op_kwargs["url"],
                "https://recidiviz-testing.appspot.com/validation_manager/validate",
            )
