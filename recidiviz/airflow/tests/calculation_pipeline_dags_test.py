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
