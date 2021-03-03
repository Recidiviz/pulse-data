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
"""Tests for classes in direct_ingest_raw_table_migration.py."""

import datetime
import os
import unittest

from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import (
    DeleteFromRawTableMigration,
    UpdateRawTableMigration,
    RawTableMigration,
    RAW_TABLE_MIGRATION_FILE_PREFIX,
    UPDATE_DATETIME_AGNOSTIC_DATETIME,
)
from recidiviz.utils.metadata import local_project_id_override

_DATE_1 = datetime.datetime(2020, 4, 14, 0, 31, 0)
_DATE_2 = datetime.datetime(2020, 8, 16, 1, 2, 3)


class TestDirectIngestRawTableMigration(unittest.TestCase):
    """Tests for classes in direct_ingest_raw_table_migration.py."""

    def setUp(self) -> None:
        self.region_code = "us_xx"

    def _migration_file_path_for_tag(self, raw_file_tag: str) -> str:
        return os.path.join(
            os.path.dirname(regions.__file__),
            self.region_code,
            "raw_data",
            "migrations",
            f"{RAW_TABLE_MIGRATION_FILE_PREFIX}{raw_file_tag}.py",
        )

    def test_delete_migration(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("tagC"),
            update_datetime_filters=[_DATE_1],
            filters=[("COL1", "31415")],
        )
        with local_project_id_override("recidiviz-456"):
            project_1_query_map = migration.migration_queries_by_update_datetime()

        with local_project_id_override("recidiviz-789"):
            project_2_query_map = migration.migration_queries_by_update_datetime()

        expected_query_map = {
            _DATE_1: "DELETE FROM `recidiviz-456.us_xx_raw_data.tagC` "
            "WHERE COL1 = '31415' AND update_datetime = '2020-04-14T00:31:00';"
        }
        self.assertEqual(expected_query_map, project_1_query_map)

        expected_query_map = {
            _DATE_1: "DELETE FROM `recidiviz-789.us_xx_raw_data.tagC` "
            "WHERE COL1 = '31415' AND update_datetime = '2020-04-14T00:31:00';"
        }
        self.assertEqual(expected_query_map, project_2_query_map)

    def test_delete_migration_multiple_filters_and_dates(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("file_tag_first"),
            update_datetime_filters=[_DATE_1, _DATE_2],
            filters=[("col_name_1a", "31415"), ("col_name_1b", "45678")],
        )
        with local_project_id_override("recidiviz-456"):
            query_map = migration.migration_queries_by_update_datetime()

        expected_query_map = {
            _DATE_1: "DELETE FROM `recidiviz-456.us_xx_raw_data.file_tag_first` "
            "WHERE col_name_1a = '31415' AND col_name_1b = '45678' "
            "AND update_datetime = '2020-04-14T00:31:00';",
            _DATE_2: "DELETE FROM `recidiviz-456.us_xx_raw_data.file_tag_first` "
            "WHERE col_name_1a = '31415' AND col_name_1b = '45678' "
            "AND update_datetime = '2020-08-16T01:02:03';",
        }
        self.assertEqual(expected_query_map, query_map)

    def test_delete_migration_update_datetime_agnostic(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("tagC"),
            update_datetime_filters=None,
            filters=[("COL1", "31415")],
        )
        with local_project_id_override("recidiviz-456"):
            project_1_query_map = migration.migration_queries_by_update_datetime()

        with local_project_id_override("recidiviz-789"):
            project_2_query_map = migration.migration_queries_by_update_datetime()

        expected_query_map = {
            UPDATE_DATETIME_AGNOSTIC_DATETIME: """DELETE FROM `recidiviz-456.us_xx_raw_data.tagC` WHERE COL1 = '31415';"""
        }
        self.assertEqual(expected_query_map, project_1_query_map)

        expected_query_map = {
            UPDATE_DATETIME_AGNOSTIC_DATETIME: """DELETE FROM `recidiviz-789.us_xx_raw_data.tagC` WHERE COL1 = '31415';"""
        }
        self.assertEqual(expected_query_map, project_2_query_map)

    def test_update_migration(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("tagC"),
            update_datetime_filters=[
                _DATE_2,
            ],
            filters=[("COL1", "31415")],
            updates=[("COL1", "91011")],
        )
        with local_project_id_override("recidiviz-456"):
            project_1_query_map = migration.migration_queries_by_update_datetime()

        with local_project_id_override("recidiviz-789"):
            project_2_query_map = migration.migration_queries_by_update_datetime()

        expected_query_map = {
            _DATE_2: """UPDATE `recidiviz-456.us_xx_raw_data.tagC` SET COL1 = '91011' WHERE COL1 = '31415' AND update_datetime = '2020-08-16T01:02:03';"""
        }
        self.assertEqual(expected_query_map, project_1_query_map)

        expected_query_map = {
            _DATE_2: """UPDATE `recidiviz-789.us_xx_raw_data.tagC` SET COL1 = '91011' WHERE COL1 = '31415' AND update_datetime = '2020-08-16T01:02:03';"""
        }
        self.assertEqual(expected_query_map, project_2_query_map)

    def test_update_migration_multiples(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("file_tag_first"),
            update_datetime_filters=[_DATE_1, _DATE_2],
            filters=[("col_name_1a", "12345"), ("col_name_1b", "4567")],
            updates=[("col_name_1a", "4567"), ("col_name_1b", "12345")],
        )
        with local_project_id_override("recidiviz-456"):
            query_map = migration.migration_queries_by_update_datetime()

        expected_query_map = {
            _DATE_1: """UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` SET col_name_1a = '4567', col_name_1b = '12345' WHERE col_name_1a = '12345' AND col_name_1b = '4567' AND update_datetime = '2020-04-14T00:31:00';""",
            _DATE_2: """UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` SET col_name_1a = '4567', col_name_1b = '12345' WHERE col_name_1a = '12345' AND col_name_1b = '4567' AND update_datetime = '2020-08-16T01:02:03';""",
        }
        self.assertEqual(expected_query_map, query_map)

    def test_update_migration_update_datetime_agnostic(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("tagC"),
            update_datetime_filters=None,
            filters=[("COL1", "31415")],
            updates=[("COL1", "91011")],
        )
        with local_project_id_override("recidiviz-456"):
            project_1_query_map = migration.migration_queries_by_update_datetime()

        with local_project_id_override("recidiviz-789"):
            project_2_query_map = migration.migration_queries_by_update_datetime()

        expected_query_map = {
            UPDATE_DATETIME_AGNOSTIC_DATETIME: "UPDATE `recidiviz-456.us_xx_raw_data.tagC` SET COL1 = '91011' WHERE COL1 = '31415';"
        }
        self.assertEqual(expected_query_map, project_1_query_map)

        expected_query_map = {
            UPDATE_DATETIME_AGNOSTIC_DATETIME: "UPDATE `recidiviz-789.us_xx_raw_data.tagC` SET COL1 = '91011' WHERE COL1 = '31415';"
        }
        self.assertEqual(expected_query_map, project_2_query_map)

    def test_print_migrations(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1, _DATE_2],
                filters=[("COL1", "31415")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[
                    _DATE_1,
                ],
                filters=[("COL1", "31415")],
                updates=[("COL1", "91011")],
            ),
        ]

        # Shouldn't crash
        with local_project_id_override("recidiviz-456"):
            RawTableMigration.print_list(migrations)
