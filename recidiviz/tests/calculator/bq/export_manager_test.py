# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Tests for export_manager.py."""

import collections
from itertools import chain
import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.calculator.bq import export_manager
from recidiviz.calculator.bq.export_manager import ModuleType


class ExportManagerTestCounty(unittest.TestCase):
    """Tests for export_manager.py."""


    def setUp(self):
        self.module = ModuleType.COUNTY

        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_name = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_name)
        self.mock_table_id = 'test_table'
        self.mock_table_query = 'SELECT NULL LIMIT 0'

        self.bq_load_patcher = mock.patch(
            'recidiviz.calculator.bq.export_manager.bq_load')
        self.mock_bq_load = self.bq_load_patcher.start()

        self.client_patcher = mock.patch(
            'recidiviz.calculator.bq.export_manager.bq_utils.client')
        self.mock_client = self.client_patcher.start().return_value

        self.cloudsql_export_patcher = mock.patch(
            'recidiviz.calculator.bq.export_manager.cloudsql_export')
        self.mock_cloudsql_export = self.cloudsql_export_patcher.start()

        Table = collections.namedtuple('Table', ['name'])
        test_tables = [Table('first_table'), Table('second_table')]
        export_config_values = {
            'COUNTY_BASE_TABLES_BQ_DATASET': self.mock_dataset_name,
            'COUNTY_TABLES_TO_EXPORT': test_tables,
            'COUNTY_TABLE_EXPORT_QUERIES': {
                self.mock_table_id: self.mock_table_query,
                **{table.name: self.mock_table_query for table in test_tables}
            }
        }
        self.export_config_patcher = mock.patch(
            'recidiviz.calculator.bq.export_manager.export_config',
            **export_config_values)
        self.mock_export_config = self.export_config_patcher.start()


    def tearDown(self):
        self.bq_load_patcher.stop()
        self.client_patcher.stop()
        self.cloudsql_export_patcher.stop()
        self.export_config_patcher.stop()


    def test_export_table_then_load_table_dataset(self):
        """Test that export_table_then_load_table uses a dataset if specified.
        """
        table = 'first_table'
        dataset = self.mock_client.dataset('random_dataset')

        export_manager.export_table_then_load_table(table, dataset, self.module)

        self.mock_bq_load.start_table_load_and_wait.assert_called_with(
            dataset, table, self.module)


    def test_export_table_then_load_table_doesnt_load(self):
        """Test that export_table_then_load_table doesn't load if export fails.
        """
        self.mock_cloudsql_export.export_table.return_value = False

        with self.assertLogs(level='ERROR'):
            export_manager.export_table_then_load_table('random-table',
                                                        self.mock_dataset,
                                                        self.module)

        self.mock_bq_load.assert_not_called()


    def test_export_then_load_all_sequentially(self):
        """Test that tables are exported then loaded sequentially."""
        default_dataset = self.mock_client.dataset(
            self.mock_export_config.COUNTY_BASE_TABLES_BQ_DATASET)

        # Suppose all exports succeed.
        self.mock_cloudsql_export.export_table.side_effect = (
            [True]*len(self.mock_export_config.COUNTY_TABLES_TO_EXPORT))

        mock_parent = mock.Mock()
        mock_parent.attach_mock(
            self.mock_cloudsql_export.export_table, 'export')
        mock_parent.attach_mock(
            self.mock_bq_load.start_table_load_and_wait, 'load')

        export_then_load_calls = list(chain.from_iterable([
            (mock.call.export(table.name,
                              self.mock_export_config.
                              COUNTY_TABLE_EXPORT_QUERIES[table.name]),
             mock.call.load(default_dataset, table.name, self.module))
            for table in self.mock_export_config.COUNTY_TABLES_TO_EXPORT
        ]))

        export_manager.export_then_load_all_sequentially(self.module)

        mock_parent.assert_has_calls(export_then_load_calls)


    def test_export_then_load_all_sequentially_fails_invalid_module(self):
        with self.assertLogs(level='ERROR'):
            export_manager.export_then_load_all_sequentially('nonsense')


    def test_export_all_then_load_all(self):
        """Test that export_all_then_load_all exports all tables then loads all
            tables.
        """
        default_dataset = self.mock_client.dataset(
            self.mock_export_config.COUNTY_BASE_TABLES_BQ_DATASET)

        mock_parent = mock.Mock()
        mock_parent.attach_mock(
            self.mock_cloudsql_export.export_all_tables, 'export_all')
        mock_parent.attach_mock(
            self.mock_bq_load.load_all_tables_concurrently, 'load_all')

        export_all_then_load_all_calls = [
            mock.call.export_all(
                self.mock_export_config.COUNTY_TABLES_TO_EXPORT,
                self.mock_export_config.COUNTY_TABLE_EXPORT_QUERIES),
            mock.call.load_all(
                default_dataset,
                self.mock_export_config.COUNTY_TABLES_TO_EXPORT, self.module)
        ]

        export_manager.export_all_then_load_all(self.module)

        mock_parent.assert_has_calls(export_all_then_load_all_calls)


    def test_export_all_then_load_all_fails_invalid_module(self):
        with self.assertLogs(level='ERROR'):
            export_manager.export_all_then_load_all('nonsense')
