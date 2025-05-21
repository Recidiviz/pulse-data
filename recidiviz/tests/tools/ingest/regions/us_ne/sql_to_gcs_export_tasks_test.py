# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the SQL to GCS export tasks."""
import unittest

from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks import (
    UsNeDatabaseName,
    UsNeSqltoGCSExportTask,
)


class TestUsNeSqltoGCSExportTasks(unittest.TestCase):
    """Tests for the SQL to GCS export task factory."""

    def test_sql_to_gcs_export_task_factory(self) -> None:
        file_tags = ["test_file_1", "PIMSSanctions", "E04_LOCT_PRFX"]
        tasks = [
            UsNeSqltoGCSExportTask.from_file_tag(file_tag) for file_tag in file_tags
        ]

        self.assertEqual(len(tasks), 3)
        # Should default to DCS_WEB and table name same as file tag
        self.assertEqual(tasks[0].file_tag, "test_file_1")
        self.assertEqual(tasks[0].table_name, "test_file_1")
        self.assertEqual(tasks[0].db, UsNeDatabaseName.DCS_WEB)

        self.assertEqual(tasks[1].file_tag, "PIMSSanctions")
        self.assertEqual(tasks[1].table_name, "view_PIMSSanctions")
        self.assertEqual(tasks[1].db, UsNeDatabaseName.DCS_WEB)

        self.assertEqual(tasks[2].file_tag, "E04_LOCT_PRFX")
        self.assertEqual(tasks[2].table_name, "E04_LOCT_PRFX")
        self.assertEqual(tasks[2].db, UsNeDatabaseName.DCS_MVS)
