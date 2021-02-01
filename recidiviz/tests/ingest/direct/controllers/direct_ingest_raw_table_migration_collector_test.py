"""Tests for the DirectIngestRawTableMigrationCollector class."""

import datetime
import unittest
from unittest.mock import patch, Mock

from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration_collector import \
    DirectIngestRawTableMigrationCollector
from recidiviz.tests.ingest.direct.controllers import fixtures


DATE_1 = datetime.datetime(2020, 6, 10, 0, 0)
DATE_2 = datetime.datetime(2020, 9, 21, 0, 0)


@patch('recidiviz.utils.metadata.project_id', Mock(return_value='recidiviz-456'))
class TestDirectIngestRawTableMigrationCollector(unittest.TestCase):
    """Tests for the DirectIngestRawTableMigrationCollector class."""

    def test_collect_queries(self) -> None:
        collector = DirectIngestRawTableMigrationCollector(region_code='us_xx', regions_module_override=fixtures)
        queries_map = collector.collect_raw_table_migration_queries()

        expected_queries_map = {
            ('file_tag_first', DATE_1): [
                "UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` SET column_1b = '456' "
                "WHERE update_datetime = '2020-06-10T00:00:00' AND column_1a = '123';"
            ],
            ('file_tag_first', DATE_2): [
                "UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` SET column_1b = '456' "
                "WHERE update_datetime = '2020-09-21T00:00:00' AND column_1a = '123';",
                "DELETE FROM `recidiviz-456.us_xx_raw_data.file_tag_first` "
                "WHERE update_datetime = '2020-09-21T00:00:00' AND column_1a = '00000000';"
            ],
            ('tagC', DATE_1): [
                "UPDATE `recidiviz-456.us_xx_raw_data.tagC` SET COL1 = '456' "
                "WHERE update_datetime = '2020-06-10T00:00:00' AND COL1 = '123';"
            ],
            ('tagC', DATE_2): [
                "UPDATE `recidiviz-456.us_xx_raw_data.tagC` SET COL1 = '456' "
                "WHERE update_datetime = '2020-09-21T00:00:00' AND COL1 = '123';"
            ]
        }
        self.assertEqual(expected_queries_map, queries_map)
