#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for the Workflows ETL delegate."""
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsETLDelegate


class TestDelegate(WorkflowsETLDelegate):
    STATE_CODE = "US_XX"
    EXPORT_FILENAME = "export_filename.json"

    def run_etl(self) -> None:
        pass


class TestWorkflowsETLDelegate(TestCase):
    """Tests for the Workflows ETL delegate."""

    def test_filename_matches_matches_name(self) -> None:
        """Test that the filename matcher does not ignore the file format."""
        delegate = TestDelegate()
        self.assertTrue(delegate.filename_matches("export_filename.json"))
        self.assertFalse(delegate.filename_matches("some_other_file.json"))

    def test_filename_matches_matches_extension(self) -> None:
        """Test that the filename matcher does not ignore the file format."""
        delegate = TestDelegate()
        self.assertTrue(delegate.filename_matches("export_filename.json"))
        self.assertFalse(delegate.filename_matches("export_filename.csv"))

    def test_get_filepath_uses_project_id(self) -> None:
        """Tests that get_filepath() incorporates the current project ID."""
        with local_project_id_override("test-project"):
            delegate = TestDelegate()
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="test-project-practices-etl-data",
                    blob_name="US_XX/export_filename.json",
                ),
                delegate.get_filepath(),
            )
