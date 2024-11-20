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
"""Unit test for the monitoring DAG"""
from unittest.mock import patch

from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestMonitoringDag(AirflowIntegrationTest):
    """Tests the airflow monitoring dag."""

    def test_import(self) -> None:
        """Just tests that the monitoring_dag file can be imported."""
        # Need to import monitoring_dag inside test suite so environment variables are
        # set before importing, otherwise monitoring_dag will raise an Error and not
        # import.

        # pylint: disable=C0415 import-outside-toplevel
        # pylint: disable=unused-import
        from recidiviz.airflow.dags.monitoring_dag import monitoring_dag

        # If nothing fails, this test passes
