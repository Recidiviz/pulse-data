# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for types defined in direct_ingest_big_query_view_types_test.py"""
import unittest

from mock import patch

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestRawDataTableLatestBigQueryView, RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE, \
    DirectIngestRawDataTableUpToDateBigQueryView, RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRawFileConfig


class DirectIngestBigQueryViewTypesTest(unittest.TestCase):
    """Tests for types defined in direct_ingest_big_query_view_types_test.py"""

    PROJECT_ID = 'project-id'

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID

    def tearDown(self):
        self.metadata_patcher.stop()

    def test_raw_latest_view(self):
        view = DirectIngestRawDataTableLatestBigQueryView(
            region_code='us_xx',
            raw_file_config=DirectIngestRawFileConfig(
                file_tag='table_name',
                primary_key_cols=['col1', 'col2']
            )
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('us_xx_raw_data_up_to_date_views', view.dataset_id)
        self.assertEqual('table_name_latest', view.table_id)
        self.assertEqual('table_name_latest', view.view_id)

        expected_view_query = RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE.format(
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str='col1, col2',
            raw_table_dataset_id='us_xx_raw_data',
            raw_table_name='table_name'
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual('SELECT * FROM `project-id.us_xx_raw_data_up_to_date_views.table_name_latest`',
                         view.select_query)

    def test_raw_up_to_date_view(self):
        view = DirectIngestRawDataTableUpToDateBigQueryView(
            region_code='us_xx',
            raw_file_config=DirectIngestRawFileConfig(
                file_tag='table_name',
                primary_key_cols=['col1']
            )
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('us_xx_raw_data_up_to_date_views', view.dataset_id)
        self.assertEqual('table_name_by_update_date', view.table_id)
        self.assertEqual('table_name_by_update_date', view.view_id)

        expected_view_query = RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE.format(
            project_id=self.PROJECT_ID,
            raw_table_primary_key_str='col1',
            raw_table_dataset_id='us_xx_raw_data',
            raw_table_name='table_name'
        )

        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual('SELECT * FROM `project-id.us_xx_raw_data_up_to_date_views.table_name_by_update_date`',
                         view.select_query)
