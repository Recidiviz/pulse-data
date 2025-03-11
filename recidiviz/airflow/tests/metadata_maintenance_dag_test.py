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
"""Unit test for the metadata maintenance DAG"""
from unittest.mock import patch

from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestMetadataMaintenanceDag(AirflowIntegrationTest):
    """Tests the metadata_maintenance DAG"""

    def test_import(self) -> None:
        """Just tests that the metadata_maintenance file can be imported."""
        # Need to import metadata_maintenance_dag inside test suite so environment variables are
        # set before importing, otherwise metadata_maintenance_dag will raise an Error and not
        # import.

        # pylint: disable=C0415 import-outside-toplevel
        # pylint: disable=unused-import

        # If nothing fails, this test passes
