# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

"""Tests for export_config.py."""

import string
import unittest
from unittest import mock

import sqlalchemy

from recidiviz.calculator.bq import export_config


class ExportConfigTest(unittest.TestCase):
    """Tests for export_config.py."""


    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        metadata_values = {
            'project_id.return_value': self.mock_project_id,
        }
        self.metadata_patcher = mock.patch(
            'recidiviz.calculator.bq.export_config.metadata',
            **metadata_values)
        self.metadata_patcher.start()


    def test_TABLES_TO_EXPORT_types(self):
        """Make sure that all TABLES_TO_EXPORT are of type sqlalchemy.Table."""
        for table in export_config.TABLES_TO_EXPORT:
            self.assertIsInstance(table, sqlalchemy.Table)


    def test_gcs_export_uri(self):
        """Test that gcs_expor_uri generates a GCS URI
            with the correct project ID and table name.
        """
        fake_table = 'my_fake_table'
        bucket = '{}-dbexport'.format(self.mock_project_id)
        gcs_export_uri = 'gs://{bucket}/{table_name}.csv'.format(
            bucket=bucket, table_name=fake_table)

        self.assertEqual(
            gcs_export_uri, export_config.gcs_export_uri(fake_table))


    def test_BASE_TABLES_BQ_DATASET(self):
        """Make sure BASE_TABLES_BQ_DATASET is defined correctly.

            Checks that it is a string, checks that it has characters,
            and checks that those characters are letters, numbers, or _.
        """
        self.assertIsInstance(export_config.BASE_TABLES_BQ_DATASET, str)

        self.assertTrue(len(export_config.BASE_TABLES_BQ_DATASET) > 0)

        allowed_characters = set(string.ascii_letters + string.digits + '_')
        self.assertTrue(
            set(export_config.BASE_TABLES_BQ_DATASET)
            .issubset(allowed_characters),
            msg='BigQuery Dataset names must only contain letters, numbers,'
            ' and underscores. Check `export_config.BASE_TABLES_BQ_DATASET`.'
        )


    def test_COLUMNS_TO_EXCLUDE_typos(self):
        """Make sure COLUMNS_TO_EXCLUDE are defined correctly in case of typos.

        1) Check that all tables are defined in
            export_config.TABLES_TO_EXPORT.

        2) Check that all columns are defined in their respective tables.
        """

        for table in export_config.COLUMNS_TO_EXCLUDE.keys(): # pylint: disable=consider-iterating-dictionary
            self.assertTrue(
                table in export_config.ALL_TABLE_COLUMNS.keys(),
                msg='Table "{}" in `export_config.COLUMNS_TO_EXCLUDE`'
                    ' not found in `export_config.TABLES_TO_EXPORT`.'
                    ' Did you spell it correctly?'.format(table)
            )

        for table, columns in export_config.COLUMNS_TO_EXCLUDE.items():
            for column in columns:
                self.assertTrue(
                    column in export_config.ALL_TABLE_COLUMNS.get(table),
                    msg='Column "{}" in `export_config.COLUMNS_TO_EXCLUDE`'
                    ' not found in table "{}".'
                    ' Did you spell it correctly?'.format(column, table)
                )


    def test_COLUMNS_TO_EXCLUDE_excluded(self):
        """Make sure COLUMNS_TO_EXCLUDE are excluded from
            TABLE_COLUMNS_TO_EXPORT and other derived values such as
            TABLE_EXPORT_QUERIES and TABLE_EXPORT_SCHEMA.
        """
        # Make sure COLUMNS_TO_EXCLUDE are excluded from TABLE_COLUMNS_TO_EXPORT
        for table, columns in export_config.COLUMNS_TO_EXCLUDE.items():
            for column in columns:
                self.assertFalse(
                    column in export_config.TABLE_COLUMNS_TO_EXPORT.get(table),
                    msg='Column "{}" not excluded properly from table "{}".'
                    ' Check export_config.TABLE_COLUMNS_TO_EXPORT'.format(
                        column, table)
                )

        # Make sure COLUMNS_TO_EXCLUDE are excluded from TABLE_EXPORT_QUERIES.
        for table, columns in export_config.COLUMNS_TO_EXCLUDE.items():
            table_query = export_config.TABLE_EXPORT_QUERIES.get(table)
            for column in columns:
                search_pattern = '[ ]*{}[, ]+'.format(column)
                self.assertNotRegex(
                    table_query, search_pattern,
                    msg='Column "{}" not excluded properly from table "{}".'
                        ' Check export_config.TABLE_COLUMNS_TO_EXPORT and'
                        ' export_config.TABLE_EXPORT_QUERIES'.format(
                            column, table)
                )

        # Make sure COLUMNS_TO_EXCLUDE are excluded from TABLE_EXPORT_SCHEMA.
        for table, columns in export_config.COLUMNS_TO_EXCLUDE.items():
            table_schema_columns = [
                column['name']
                for column in export_config.TABLE_EXPORT_SCHEMA.get(table)
            ]
            for column in columns:
                self.assertFalse(
                    column in table_schema_columns,
                    msg='Column "{}" not excluded properly from table "{}".'
                        ' Check export_config.TABLE_COLUMNS_TO_EXPORT and'
                        ' export_config.TABLE_EXPORT_SCHEMA'.format(
                            column, table)
                )


    def test_COLUMNS_TO_EXCLUDE_all_excluded(self):
        """Make sure a table is removed if all its columns are excluded."""
        for table, columns in export_config.COLUMNS_TO_EXCLUDE.items():
            self.assertFalse(
                set(export_config.ALL_TABLE_COLUMNS.get(table))
                .issubset(columns),
                msg='All columns from table {} are excluded. '
                ' Remove the table from'
                ' export_config.TABLES_TO_EXPORT.'.format(table)
            )
