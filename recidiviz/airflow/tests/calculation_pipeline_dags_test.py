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

dag_folder = "dags"


@patch(
    "os.environ",
    {
        "GCP_PROJECT_ID": "recidiviz-testing",
        "CONFIG_FILE": "../calculator/pipeline/calculation_pipeline_templates.yaml",
    },
)
class TestDagIntegrity(unittest.TestCase):
    def test_dagbag_import(self) -> None:
        """
        Verify that Airflow will be able to import all DAGs in the repository without errors
        """
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        self.assertEqual(
            len(dagbag.import_errors),
            0,
            f"There should be no DAG failures. Got: {dagbag.import_errors}",
        )

    def test_correct_dag(self) -> None:
        """
        Verify that the DAGs discovered have the correct name
        """
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        self.assertEqual(len(dagbag.dag_ids), 2)
        self.assertEqual(
            dagbag.dag_ids,
            [
                "recidiviz-testing_incremental_calculation_pipeline_dag",
                "recidiviz-testing_historical_calculation_pipeline_dag",
            ],
        )
