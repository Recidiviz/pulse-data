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
import csv
import datetime
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks import (
    UsNeDatabaseName,
    UsNeSqltoGCSExportTask,
)


class TestUsNeSqltoGCSExportTasks(unittest.TestCase):
    """Tests for the SQL to GCS export task factory."""

    def test_sql_to_gcs_export_task_factory(self) -> None:
        file_tags = ["PIMSSanctions", "E04_LOCT_PRFX"]
        update_dt = datetime.datetime(2023, 1, 1)
        region_raw_file_config = DirectIngestRegionRawFileConfig(StateCode.US_NE.value)
        tasks = [
            UsNeSqltoGCSExportTask.for_file_tag(
                file_tag,
                update_dt,
                raw_file_config=region_raw_file_config.raw_file_configs[file_tag],
            )
            for file_tag in file_tags
        ]

        self.assertEqual(len(tasks), 2)

        self.assertEqual(tasks[0].file_tag, "PIMSSanctions")
        self.assertEqual(tasks[0].table_name, "view_PIMSSanctions")
        # DCS_WEB is the default database
        self.assertEqual(tasks[0].db, UsNeDatabaseName.DCS_WEB)
        self.assertEqual(
            tasks[0].file_name,
            "unprocessed_2023-01-01T00:00:00:000000_raw_PIMSSanctions.csv",
        )

        self.assertEqual(tasks[1].file_tag, "E04_LOCT_PRFX")
        # Defaults to table name the same as file tag
        self.assertEqual(tasks[1].table_name, "E04_LOCT_PRFX")
        self.assertEqual(tasks[1].db, UsNeDatabaseName.DCS_MVS)
        self.assertEqual(
            tasks[1].file_name,
            "unprocessed_2023-01-01T00:00:00:000000_raw_E04_LOCT_PRFX.csv",
        )

        task = UsNeSqltoGCSExportTask.for_qualified_table_name(
            qualified_table_name="DCS_MVS.test_file_1",
            update_datetime=update_dt,
            region_raw_file_config=region_raw_file_config,
        )
        # Should set file tag the same as table name
        self.assertEqual(task.file_tag, "test_file_1")
        self.assertEqual(task.table_name, "test_file_1")
        self.assertEqual(task.db, UsNeDatabaseName.DCS_MVS)
        self.assertEqual(
            task.file_name,
            "unprocessed_2023-01-01T00:00:00:000000_raw_test_file_1.csv",
        )
        self.assertEqual(task.columns, ["*"])

        default_region_config = region_raw_file_config.default_config()
        self.assertEqual(task.encoding, default_region_config.default_encoding)
        self.assertEqual(
            task.line_terminator,
            default_region_config.default_custom_line_terminator,
        )
        self.assertEqual(task.delimiter, default_region_config.default_separator)
        self.assertEqual(
            task.quoting_mode,
            (
                csv.QUOTE_NONE
                if default_region_config.default_ignore_quotes
                else csv.QUOTE_MINIMAL
            ),
        )
