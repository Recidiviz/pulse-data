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
Unit test to ensure that the DAGs are valid and will be properly loaded into the Airflow UI.
"""
import unittest
from unittest.mock import patch

from airflow.models import DagBag

from recidiviz.airflow.tests.calculation_pipeline_dags_test import (
    CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER


@patch(
    "os.environ",
    {
        "GCP_PROJECT": "recidiviz-testing",
        "CONFIG_FILE": CALC_PIPELINE_CONFIG_FILE_RELATIVE_PATH,
    },
)
class TestDagIntegrity(unittest.TestCase):
    """Tests the dags defined in the /dags package."""

    def test_dag_bag_import(self) -> None:
        """
        Verify that Airflow will be able to import all DAGs in the repository without errors
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertEqual(
            len(dag_bag.import_errors),
            0,
            f"There should be no DAG failures. Got: {dag_bag.import_errors}",
        )

    def test_correct_dag(self) -> None:
        """
        Verify that the DAGs discovered have the correct name
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertEqual(len(dag_bag.dag_ids), 2)
        self.assertSetEqual(
            set(dag_bag.dag_ids),
            {
                "recidiviz-testing_incremental_calculation_pipeline_dag",
                "recidiviz-testing_historical_calculation_pipeline_dag",
            },
        )
