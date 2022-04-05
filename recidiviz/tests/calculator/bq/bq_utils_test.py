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

"""Tests for bq_utils.py."""

import unittest
from unittest import mock

from google.cloud import bigquery
from google.cloud import exceptions

from recidiviz.calculator.bq import bq_utils
from recidiviz.calculator.bq.views import bqview


class BqUtilsTest(unittest.TestCase):
    """Tests for bq_load.py."""


    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'fake-dataset'
        self.mock_table_id = 'test_table'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)
        self.mock_table = self.mock_dataset.table(self.mock_table_id)

        self.client_patcher = mock.patch(
            'recidiviz.calculator.bq.bq_utils.client')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_view = bqview.BigQueryView(
            view_id='test_view',
            view_query='SELECT NULL LIMIT 0'
        )


    def tearDown(self):
        self.client_patcher.stop()


    def test_create_dataset_if_necessary(self):
        """Check that a dataset is created if it does not exist."""
        self.mock_client.get_dataset.side_effect = exceptions.NotFound('!')
        bq_utils.create_dataset_if_necessary(self.mock_dataset)
        self.mock_client.create_dataset.assert_called()


    def test_create_dataset_if_necessary_dataset_exists(self):
        """Check that a dataset is not created if it already exists."""
        self.mock_client.get_dataset.side_effect = None
        bq_utils.create_dataset_if_necessary(self.mock_dataset)
        self.mock_client.create_dataset.assert_not_called()


    def test_table_exists(self):
        """Check that table_exists returns True if the table exists."""
        self.mock_client.get_table.side_effect = None
        self.assertTrue(
            bq_utils.table_exists(self.mock_dataset, self.mock_table_id))


    def test_table_exists_does_not_exist(self):
        """Check that table_exists returns False if the table does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='WARNING'):
            table_exists = bq_utils.table_exists(
                self.mock_dataset, self.mock_table_id)
            self.assertFalse(table_exists)


    def test_create_or_update_view_creates_view(self):
        """create_or_update_view creates a View if it does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        bq_utils.create_or_update_view(self.mock_dataset, self.mock_view)
        self.mock_client.create_table.assert_called()
        self.mock_client.update_table.assert_not_called()


    def test_create_or_update_view_updates_view(self):
        """create_or_update_view updates a View if it already exist."""
        self.mock_client.get_table.side_effect = None
        bq_utils.create_or_update_view(self.mock_dataset, self.mock_view)
        self.mock_client.update_table.assert_called()
        self.mock_client.create_table.assert_not_called()
