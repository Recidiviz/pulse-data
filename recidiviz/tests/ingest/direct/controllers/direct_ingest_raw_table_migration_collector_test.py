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
from unittest.mock import Mock, patch

from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.tests.ingest.direct import fake_regions

DATE_1 = datetime.datetime(2020, 6, 10, 0, 0)
DATE_2 = datetime.datetime(2020, 9, 21, 0, 0)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestDirectIngestRawTableMigrationCollector(unittest.TestCase):
    """Tests for the DirectIngestRawTableMigrationCollector class."""

    def test_collect_queries(self) -> None:
        collector = DirectIngestRawTableMigrationCollector(
            region_code="us_xx", regions_module_override=fake_regions
        )
        queries_map = collector.collect_raw_table_migration_queries()

        file_tag_first_query_1 = """UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` original
SET column_1b = updates.new__column_1b
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS column_1a, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__column_1b),
    STRUCT('123' AS column_1a, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__column_1b)
])) updates
WHERE original.column_1a = updates.column_1a AND original.update_datetime = updates.update_datetime;"""

        file_tag_first_query_2 = """DELETE FROM `recidiviz-456.us_xx_raw_data.file_tag_first`
WHERE STRUCT(column_1a, update_datetime) IN (
    STRUCT(\"00000000\", \"2020-09-21T00:00:00\")
);"""

        tagC_query_1 = """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1),
    STRUCT('123' AS COL1, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"""

        tagC_query_2 = """DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1) IN (
    STRUCT(\"789\")
);"""

        expected_queries_map = {
            "file_tag_first": [file_tag_first_query_1, file_tag_first_query_2],
            "tagC": [tagC_query_1, tagC_query_2],
        }

        self.assertEqual(expected_queries_map, queries_map)
