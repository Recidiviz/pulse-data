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
"""Tests for the BigQueryColumnChecker class."""
from unittest import TestCase, mock

from google.cloud import bigquery

from recidiviz.ingest.views.metadata_helpers import (
    BigQueryTableChecker,
)


class TestBigQueryColumnChecker(TestCase):
    """Tests for BigQueryColumnChecker."""

    def setUp(self) -> None:
        self.client_patcher = mock.patch('recidiviz.ingest.views.metadata_helpers.BigQueryClientImpl')
        self.client_fn = self.client_patcher.start()

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'test-project'

        self.checker = BigQueryTableChecker('fake-dataset', 'fake-table')

        self.mock_table = mock.MagicMock()
        self.mock_table.schema = [
            bigquery.SchemaField('fake-col', bigquery.enums.SqlTypeNames.STRING.value),
            bigquery.SchemaField('fake-col-2', bigquery.enums.SqlTypeNames.STRING.value),
        ]

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_column_in_table(self) -> None:
        self.client_fn().get_table.return_value = self.mock_table
        self.assertTrue(self.checker.get_has_column_predicate('fake-col')())
        self.assertTrue(self.checker.get_has_column_predicate('fake-col-2')())
        self.client_fn().get_table.assert_called_once()

    def test_column_not_in_table(self) -> None:
        self.client_fn().get_table.return_value = self.mock_table
        self.assertFalse(self.checker.get_has_column_predicate('fake-column')())
