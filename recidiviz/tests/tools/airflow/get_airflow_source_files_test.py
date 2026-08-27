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
"""Tests for get_airflow_source_files."""
import unittest

from recidiviz.tools.airflow.get_airflow_source_files import main


class TestGetAirflowSourceFiles(unittest.TestCase):
    """Tests for get_airflow_source_files."""

    def test_airflow_dag_routing(self) -> None:
        with self.assertLogs(level="INFO") as captured:
            main(dry_run=True, output_path="")

        # recidiviz/utils/airflow_dag.py routes to its recidiviz/utils/ location
        self.assertIn(
            "INFO:root:Source file: recidiviz/utils/airflow_dag.py, destination: recidiviz/utils/airflow_dag.py",
            captured.output,
        )

        # recidiviz/airflow/calculation_dag.py routes to the root directory
        self.assertIn(
            "INFO:root:Source file: recidiviz/airflow/dags/calculation_dag.py, destination: calculation_dag.py",
            captured.output,
        )
