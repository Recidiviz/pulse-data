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
Unit test to ensure that the DAG is valid and will be properly loaded into the Airflow UI.
"""
import unittest
from unittest.mock import patch

from airflow.models import DagBag

from recidiviz.airflow.dags.utils.export_tasks_config import PIPELINE_AGNOSTIC_EXPORTS

dag_folder = "dags"


# Exports that do not rely on the completion of pipelines
NORMALIZED_STATE_AGNOSTIC_EXPORTS = [
    f"trigger_{export_name.lower()}_bq_metric_export"
    for export_name in PIPELINE_AGNOSTIC_EXPORTS
]


@patch(
    "os.environ",
    {
        "GCP_PROJECT": "recidiviz-testing",
        "CONFIG_FILE": "../calculator/pipeline/calculation_pipeline_templates.yaml",
    },
)
class TestDagIntegrity(unittest.TestCase):
    """Tests the dags defined in the /dags package."""

    def test_dag_bag_import(self) -> None:
        """
        Verify that Airflow will be able to import all DAGs in the repository without errors
        """
        dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
        self.assertEqual(
            len(dag_bag.import_errors),
            0,
            f"There should be no DAG failures. Got: {dag_bag.import_errors}",
        )

    def test_correct_dag(self) -> None:
        """
        Verify that the DAGs discovered have the correct name
        """
        dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
        self.assertEqual(len(dag_bag.dag_ids), 2)
        self.assertEqual(
            dag_bag.dag_ids,
            [
                "recidiviz-testing_incremental_calculation_pipeline_dag",
                "recidiviz-testing_historical_calculation_pipeline_dag",
            ],
        )

    def test_exports_rely_on_normalized_state(self) -> None:
        """Tests that all BQ metric exports that rely on an updated normalized_state
        dataset are downstream of the task that updates the normalized_state dataset."""
        dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

        for dag_id in dag_bag.dags:
            if dag_id != "recidiviz-testing_incremental_calculation_pipeline_dag":
                continue

            incremental_dag = dag_bag.dags[dag_id]

            normalized_state_downstream_dag = incremental_dag.partial_subset(
                task_ids_or_regex=["update_normalized_state"],
                include_downstream=True,
                include_upstream=False,
            )

            self.assertNotEqual(0, len(normalized_state_downstream_dag.task_ids))
            self.assertNotEqual(0, len(incremental_dag.task_ids))

            bq_metric_export_found = False
            for task_id in incremental_dag.task_ids:
                if "bq_metric_export" in task_id:
                    bq_metric_export_found = True

                    if task_id not in NORMALIZED_STATE_AGNOSTIC_EXPORTS:
                        self.assertIn(task_id, normalized_state_downstream_dag.task_ids)

            # This will fail if we ever change the task_id naming of the BQ metric
            # export tasks
            self.assertTrue(bq_metric_export_found)
