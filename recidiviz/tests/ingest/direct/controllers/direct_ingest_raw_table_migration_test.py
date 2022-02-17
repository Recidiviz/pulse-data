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

from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import (
    RAW_TABLE_MIGRATION_FILE_PREFIX,
    DeleteFromRawTableMigration,
    UpdateRawTableMigration,
)
from recidiviz.utils import regions
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

    def test_delete_migration_no_date_filter(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("mytag"),
            update_datetime_filters=None,
            filters=[
                ("COL2", "2171"),
                ("COL1", "31415"),
            ],
        )
        with local_project_id_override("recidiviz-789"):
            self.assertEqual(
                "recidiviz-789.us_xx_raw_data.mytag",
                migration.raw_table(sandbox_dataset_prefix=None),
            )
            self.assertEqual("mytag", migration.file_tag)
            self.assertEqual(("COL1", "COL2"), migration.ordered_filter_keys)
            self.assertEqual("31415", migration.filters["COL1"])
            self.assertEqual("2171", migration.filters["COL2"])
            self.assertEqual([("31415", "2171")], migration.ordered_filter_values)

    def test_delete_migration_date_filters(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("mytag"),
            update_datetime_filters=[_DATE_1, _DATE_2],
            filters=[
                ("COL2", "2171"),
                ("COL1", "31415"),
            ],
        )
        with local_project_id_override("recidiviz-789"):
            self.assertEqual(
                "recidiviz-789.us_xx_raw_data.mytag",
                migration.raw_table(sandbox_dataset_prefix=None),
            )
            self.assertEqual("mytag", migration.file_tag)
            self.assertEqual(
                ("COL1", "COL2", "update_datetime"), migration.ordered_filter_keys
            )
            self.assertEqual("31415", migration.filters["COL1"])
            self.assertEqual("2171", migration.filters["COL2"])

            expected_filters_values = [
                ("31415", "2171", "2020-04-14T00:31:00"),
                ("31415", "2171", "2020-08-16T01:02:03"),
            ]
            self.assertEqual(expected_filters_values, migration.ordered_filter_values)

    def test_update_migration_no_date_filter(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("mytag"),
            update_datetime_filters=None,
            filters=[
                ("COL2", "2171"),
                ("COL1", "31415"),
            ],
            updates=[
                ("COL3", "91011"),
                ("COL1", "654"),
            ],
        )
        with local_project_id_override("recidiviz-789"):
            self.assertEqual(
                "recidiviz-789.us_xx_raw_data.mytag",
                migration.raw_table(sandbox_dataset_prefix=None),
            )
            self.assertEqual("mytag", migration.file_tag)
            self.assertEqual(("COL1", "COL2"), migration.ordered_filter_keys)
            self.assertEqual("31415", migration.filters["COL1"])
            self.assertEqual("2171", migration.filters["COL2"])
            self.assertEqual([("31415", "2171")], migration.ordered_filter_values)
            self.assertEqual(("COL1", "COL3"), migration.ordered_update_keys)
            self.assertEqual("91011", migration.updates["COL3"])

    def test_update_migration_date_filters(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("mytag"),
            update_datetime_filters=[_DATE_1, _DATE_2],
            filters=[
                ("COL2", "2171"),
                ("COL1", "31415"),
            ],
            updates=[
                ("COL3", "91011"),
                ("COL1", "654"),
            ],
        )

        with local_project_id_override("recidiviz-789"):
            self.assertEqual(
                "recidiviz-789.us_xx_raw_data.mytag",
                migration.raw_table(sandbox_dataset_prefix=None),
            )
            self.assertEqual("mytag", migration.file_tag)
            self.assertEqual(
                ("COL1", "COL2", "update_datetime"), migration.ordered_filter_keys
            )
            self.assertEqual("31415", migration.filters["COL1"])
            self.assertEqual("2171", migration.filters["COL2"])

            expected_filters_values = [
                ("31415", "2171", "2020-04-14T00:31:00"),
                ("31415", "2171", "2020-08-16T01:02:03"),
            ]
            self.assertEqual(expected_filters_values, migration.ordered_filter_values)
            self.assertEqual(("COL1", "COL3"), migration.ordered_update_keys)
            self.assertEqual("91011", migration.updates["COL3"])

    def test_migration_sandbox_prefix(self) -> None:
        delete_migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("mytag"),
            update_datetime_filters=None,
            filters=[
                ("COL2", "2171"),
                ("COL1", "31415"),
            ],
        )
        update_migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("mytag"),
            update_datetime_filters=None,
            filters=[
                ("COL2", "2171"),
                ("COL1", "31415"),
            ],
            updates=[
                ("COL3", "91011"),
                ("COL1", "654"),
            ],
        )
        with local_project_id_override("recidiviz-789"):
            self.assertEqual(
                "recidiviz-789.my_prefix_us_xx_raw_data.mytag",
                delete_migration.raw_table(sandbox_dataset_prefix="my_prefix"),
            )
            self.assertEqual(
                "recidiviz-789.my_prefix_us_xx_raw_data.mytag",
                update_migration.raw_table(sandbox_dataset_prefix="my_prefix"),
            )
