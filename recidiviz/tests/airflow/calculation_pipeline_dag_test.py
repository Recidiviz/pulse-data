# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

from airflow.models import DagBag
from mock import patch

dag_folder = "recidiviz/airflow/dag"


@patch('os.environ', {'GCP_PROJECT_ID': 'recidiviz-testing',
                      'CONFIG_FILE': 'recidiviz/calculator/pipeline/production_calculation_pipeline_templates.yaml'})
@patch('recidiviz.cloud_functions.cloud_function_utils.IAP_CLIENT_ID', {'recidiviz-testing':
                                                                        'xx.apps.googleusercontent.com'})
class TestDagIntegrity(unittest.TestCase):
    def test_dagbag_import(self):
        """
        Verify that Airflow will be able to import all DAGs in the repository without errors
        """
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        self.assertEqual(
            len(dagbag.import_errors), 0,
            'There should be no DAG failures. Got: {}'.format(
                dagbag.import_errors
            )
        )

    def test_correct_dag(self):
        """
        Verify that there is one DAG with the correct name
        """
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        self.assertEqual(len(dagbag.dag_ids), 1)
        for dag_id in dagbag.dag_ids:
            self.assertEqual(dag_id, "recidiviz-testing_calculation_pipeline_dag")
