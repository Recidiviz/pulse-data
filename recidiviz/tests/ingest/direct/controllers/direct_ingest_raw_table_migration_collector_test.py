# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the DirectIngestRawTableMigrationCollector class."""

import datetime
import unittest
from unittest.mock import patch, Mock

from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import (
    UPDATE_DATETIME_AGNOSTIC_DATETIME,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.tests.ingest.direct.controllers import fixtures


DATE_1 = datetime.datetime(2020, 6, 10, 0, 0)
DATE_2 = datetime.datetime(2020, 9, 21, 0, 0)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestDirectIngestRawTableMigrationCollector(unittest.TestCase):
    """Tests for the DirectIngestRawTableMigrationCollector class."""

    def test_collect_queries(self) -> None:
        collector = DirectIngestRawTableMigrationCollector(
            region_code="us_xx", regions_module_override=fixtures
        )
        queries_map = collector.collect_raw_table_migration_queries()

        expected_queries_map = {
            ("file_tag_first", DATE_1): [
                "UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` SET column_1b = '456' "
                "WHERE column_1a = '123' AND update_datetime = '2020-06-10T00:00:00';"
            ],
            ("file_tag_first", DATE_2): [
                "UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` SET column_1b = '456' "
                "WHERE column_1a = '123' AND update_datetime = '2020-09-21T00:00:00';",
                "DELETE FROM `recidiviz-456.us_xx_raw_data.file_tag_first` "
                "WHERE column_1a = '00000000' AND update_datetime = '2020-09-21T00:00:00';",
            ],
            ("tagC", DATE_1): [
                "UPDATE `recidiviz-456.us_xx_raw_data.tagC` SET COL1 = '456' "
                "WHERE COL1 = '123' AND update_datetime = '2020-06-10T00:00:00';"
            ],
            ("tagC", DATE_2): [
                "UPDATE `recidiviz-456.us_xx_raw_data.tagC` SET COL1 = '456' "
                "WHERE COL1 = '123' AND update_datetime = '2020-09-21T00:00:00';"
            ],
            ("tagC", UPDATE_DATETIME_AGNOSTIC_DATETIME): [
                "DELETE FROM `recidiviz-456.us_xx_raw_data.tagC` WHERE COL1 = '789';"
            ],
        }
        self.assertEqual(expected_queries_map, queries_map)
