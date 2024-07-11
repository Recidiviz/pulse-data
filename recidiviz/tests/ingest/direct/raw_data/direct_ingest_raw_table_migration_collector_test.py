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

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct import fake_regions

DATE_1 = datetime.datetime(2020, 6, 10, 0, 0)
DATE_2 = datetime.datetime(2020, 9, 21, 0, 0)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestDirectIngestRawTableMigrationCollector(unittest.TestCase):
    """Tests for the DirectIngestRawTableMigrationCollector class."""

    def setUp(self) -> None:
        self.collector = DirectIngestRawTableMigrationCollector(
            region_code="us_xx",
            instance=DirectIngestInstance.PRIMARY,
            regions_module_override=fake_regions,
        )

    def test_get_raw_table_migrations_for_file_tag(self) -> None:

        queries = self.collector.get_raw_table_migration_queries_for_file_tag(
            "file_tag_first",
            BigQueryAddress(
                dataset_id="prefixed_raw_data", table_id="file_tag_first__1"
            ),
            data_update_datetime=None,
        )

        file_tag_first_query_1 = """UPDATE `recidiviz-456.prefixed_raw_data.file_tag_first__1` original
SET column_1b = updates.new__column_1b
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS column_1a, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__column_1b),
    STRUCT('123' AS column_1a, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__column_1b)
])) updates
WHERE original.column_1a = updates.column_1a AND original.update_datetime = updates.update_datetime;"""

        file_tag_first_query_2 = """DELETE FROM `recidiviz-456.prefixed_raw_data.file_tag_first__1`
WHERE STRUCT(column_1a, update_datetime) IN (
    STRUCT(\"00000000\", \"2020-09-21T00:00:00\")
);"""

        self.assertEqual(len(queries), 2)
        self.assertIn(file_tag_first_query_1, queries)
        self.assertIn(file_tag_first_query_2, queries)

        queries = self.collector.get_raw_table_migration_queries_for_file_tag(
            "tagBasicData",
            BigQueryAddress(
                dataset_id="wacky_raw_data",
                table_id="not_related_at_all_to_tag_basic_data__2",
            ),
            data_update_datetime=None,
        )

        tagBasicData_query_1 = """UPDATE `recidiviz-456.wacky_raw_data.not_related_at_all_to_tag_basic_data__2` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1),
    STRUCT('123' AS COL1, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"""

        tagBasicData_query_2 = """DELETE FROM `recidiviz-456.wacky_raw_data.not_related_at_all_to_tag_basic_data__2`
WHERE STRUCT(COL1) IN (
    STRUCT(\"789\")
);"""

        self.assertEqual(len(queries), 2)
        self.assertIn(tagBasicData_query_1, queries)
        self.assertIn(tagBasicData_query_2, queries)

        queries = self.collector.get_raw_table_migration_queries_for_file_tag(
            "tagBasicData",
            BigQueryAddress(
                dataset_id="wacky_raw_data",
                table_id="not_related_at_all_to_tag_basic_data__2",
            ),
            data_update_datetime=DATE_1,
        )

        tagBasicData_query_1 = """UPDATE `recidiviz-456.wacky_raw_data.not_related_at_all_to_tag_basic_data__2` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1),
    STRUCT('123' AS COL1, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"""

        tagBasicData_query_2 = """DELETE FROM `recidiviz-456.wacky_raw_data.not_related_at_all_to_tag_basic_data__2`
WHERE STRUCT(COL1) IN (
    STRUCT(\"789\")
);"""

        self.assertEqual(len(queries), 2)
        self.assertIn(tagBasicData_query_1, queries)
        self.assertIn(tagBasicData_query_2, queries)

        queries = self.collector.get_raw_table_migration_queries_for_file_tag(
            "tagBasicData",
            BigQueryAddress(
                dataset_id="wacky_raw_data",
                table_id="not_related_at_all_to_tag_basic_data__2",
            ),
            data_update_datetime=DATE_1 + datetime.timedelta(minutes=1),
        )

        tagBasicData_query_2 = """DELETE FROM `recidiviz-456.wacky_raw_data.not_related_at_all_to_tag_basic_data__2`
WHERE STRUCT(COL1) IN (
    STRUCT(\"789\")
);"""

        self.assertEqual(len(queries), 1)
        self.assertIn(tagBasicData_query_2, queries)
