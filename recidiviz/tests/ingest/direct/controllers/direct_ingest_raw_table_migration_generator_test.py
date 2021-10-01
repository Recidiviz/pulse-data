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
"""Tests for classes in direct_ingest_raw_table_migration_generator.py."""

import datetime
import os
import unittest
from typing import List

from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import (
    RAW_TABLE_MIGRATION_FILE_PREFIX,
    DeleteFromRawTableMigration,
    RawTableMigration,
    UpdateRawTableMigration,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration_generator import (
    RawTableMigrationGenerator,
)
from recidiviz.utils.metadata import local_project_id_override

_DATE_1 = datetime.datetime(2020, 4, 14, 0, 31, 0)
_DATE_2 = datetime.datetime(2020, 8, 16, 1, 2, 3)


class TestDirectIngestRawTableMigrationGenerator(unittest.TestCase):
    """Tests for classes in direct_ingest_raw_table_migration_generator.py."""

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
            project_1_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        with local_project_id_override("recidiviz-789"):
            project_2_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        expected_project_1_queries = [
            """DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("31415", "2020-04-14T00:31:00")
);"""
        ]
        self.assertEqual(expected_project_1_queries, project_1_queries)

        expected_project_2_queries = [
            """DELETE FROM `recidiviz-789.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("31415", "2020-04-14T00:31:00")
);"""
        ]
        self.assertEqual(expected_project_2_queries, project_2_queries)

    def test_delete_migration_multiple_filters_and_dates(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("file_tag_first"),
            update_datetime_filters=[_DATE_1, _DATE_2],
            filters=[("col_name_1a", "31415"), ("col_name_1b", "45678")],
        )
        with local_project_id_override("recidiviz-456"):
            queries = RawTableMigrationGenerator.migration_queries([migration])

        expected_queries = [
            """DELETE FROM `recidiviz-456.us_xx_raw_data.file_tag_first`
WHERE STRUCT(col_name_1a, col_name_1b, update_datetime) IN (
    STRUCT("31415", "45678", "2020-04-14T00:31:00"),
    STRUCT("31415", "45678", "2020-08-16T01:02:03")
);"""
        ]
        self.assertEqual(expected_queries, queries)

    def test_delete_migration_update_datetime_agnostic(self) -> None:
        migration = DeleteFromRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("tagC"),
            update_datetime_filters=None,
            filters=[("COL1", "31415")],
        )
        with local_project_id_override("recidiviz-456"):
            project_1_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        with local_project_id_override("recidiviz-789"):
            project_2_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        expected_project_1_queries = [
            """DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1) IN (
    STRUCT("31415")
);"""
        ]
        self.assertEqual(expected_project_1_queries, project_1_queries)

        expected_project_2_queries = [
            """DELETE FROM `recidiviz-789.us_xx_raw_data.tagC`
WHERE STRUCT(COL1) IN (
    STRUCT("31415")
);"""
        ]
        self.assertEqual(expected_project_2_queries, project_2_queries)

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
            project_1_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        with local_project_id_override("recidiviz-789"):
            project_2_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        expected_project_1_queries = [
            """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('31415' AS COL1, CAST('2020-08-16T01:02:03' AS DATETIME) AS update_datetime, '91011' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"""
        ]
        self.assertEqual(expected_project_1_queries, project_1_queries)

        expected_project_2_queries = [
            """UPDATE `recidiviz-789.us_xx_raw_data.tagC` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('31415' AS COL1, CAST('2020-08-16T01:02:03' AS DATETIME) AS update_datetime, '91011' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"""
        ]
        self.assertEqual(expected_project_2_queries, project_2_queries)

    def test_update_migration_multiples(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("file_tag_first"),
            update_datetime_filters=[_DATE_1, _DATE_2],
            filters=[("col_name_1a", "12345"), ("col_name_1b", "4567")],
            updates=[("col_name_1a", "4567"), ("col_name_1b", "12345")],
        )
        with local_project_id_override("recidiviz-456"):
            queries = RawTableMigrationGenerator.migration_queries([migration])

        expected_queries = [
            """UPDATE `recidiviz-456.us_xx_raw_data.file_tag_first` original
SET col_name_1a = updates.new__col_name_1a, col_name_1b = updates.new__col_name_1b
FROM (SELECT * FROM UNNEST([
    STRUCT('12345' AS col_name_1a, '4567' AS col_name_1b, CAST('2020-04-14T00:31:00' AS DATETIME) AS update_datetime, '4567' AS new__col_name_1a, '12345' AS new__col_name_1b),
    STRUCT('12345' AS col_name_1a, '4567' AS col_name_1b, CAST('2020-08-16T01:02:03' AS DATETIME) AS update_datetime, '4567' AS new__col_name_1a, '12345' AS new__col_name_1b)
])) updates
WHERE original.col_name_1a = updates.col_name_1a AND original.col_name_1b = updates.col_name_1b AND original.update_datetime = updates.update_datetime;""",
        ]
        for query in queries:
            print(query)

        self.assertEqual(expected_queries, queries)

    def test_update_migration_update_datetime_agnostic(self) -> None:
        migration = UpdateRawTableMigration(
            migrations_file=self._migration_file_path_for_tag("tagC"),
            update_datetime_filters=None,
            filters=[("COL1", "31415")],
            updates=[("COL1", "91011")],
        )
        with local_project_id_override("recidiviz-456"):
            project_1_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        with local_project_id_override("recidiviz-789"):
            project_2_queries = RawTableMigrationGenerator.migration_queries(
                [migration]
            )

        expected_project_1_queries = [
            """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('31415' AS COL1, '91011' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1;"""
        ]
        self.assertEqual(expected_project_1_queries, project_1_queries)

        expected_project_2_queries = [
            """UPDATE `recidiviz-789.us_xx_raw_data.tagC` original
SET COL1 = updates.new__COL1
FROM (SELECT * FROM UNNEST([
    STRUCT('31415' AS COL1, '91011' AS new__COL1)
])) updates
WHERE original.COL1 = updates.COL1;"""
        ]
        self.assertEqual(expected_project_2_queries, project_2_queries)

    def test_print_migrations(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1, _DATE_2],
                filters=[("COL1", "31415")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL2", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[
                    _DATE_1,
                ],
                filters=[("COL1", "31415")],
                updates=[("COL1", "91011")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "31415")],
                updates=[("COL2", "91011")],
            ),
        ]

        # Shouldn't crash
        with local_project_id_override("recidiviz-456"):
            RawTableMigrationGenerator.print_list(migrations)

    def test_merge_delete_migrations(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "31415")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "1234")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("31415", "{_DATE_1.isoformat()}"),
    STRUCT("1234", "{_DATE_1.isoformat()}")
);"""
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_merge_delete_migrations_matching_filters_differnt_dates(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_2],
                filters=[("COL1", "31415")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "1234")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("31415", "{_DATE_2.isoformat()}"),
    STRUCT("1234", "{_DATE_1.isoformat()}")
);"""
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_merge_delete_migrations_one_missing_datetime_filter(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_2],
                filters=[("COL1", "31415")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "6666")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "1234")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("31415", "{_DATE_2.isoformat()}"),
    STRUCT("1234", "{_DATE_1.isoformat()}")
);""",
            """DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1) IN (
    STRUCT("6666")
);""",
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_dont_merge_delete_migrations_different_filter_keys(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "31415")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL2", "1234")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("31415", "{_DATE_1.isoformat()}")
);""",
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL2, update_datetime) IN (
    STRUCT("1234", "{_DATE_1.isoformat()}")
);""",
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_dont_merge_delete_migrations_different_filter_keys_same_values(
        self,
    ) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "1234")],
            ),
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL2", "1234")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("1234", "{_DATE_1.isoformat()}")
);""",
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL2, update_datetime) IN (
    STRUCT("1234", "{_DATE_1.isoformat()}")
);""",
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_merge_update_migrations(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "123")],
                updates=[("COL2", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "456")],
                updates=[("COL2", "4567")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, CAST('{_DATE_1.isoformat()}' AS DATETIME) AS update_datetime, '1234' AS new__COL2),
    STRUCT('456' AS COL1, CAST('{_DATE_1.isoformat()}' AS DATETIME) AS update_datetime, '4567' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"""
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_merge_update_migrations_no_datetime_filter(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "123")],
                updates=[("COL2", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "456")],
                updates=[("COL2", "4567")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, '1234' AS new__COL2),
    STRUCT('456' AS COL1, '4567' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1;"""
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_merge_update_migrations_one_missing_datetime_filter(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "123")],
                updates=[("COL2", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_2],
                filters=[("COL1", "99")],
                updates=[("COL2", "9999")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "456")],
                updates=[("COL2", "4567")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, CAST('{_DATE_1.isoformat()}' AS DATETIME) AS update_datetime, '1234' AS new__COL2),
    STRUCT('99' AS COL1, CAST('{_DATE_2.isoformat()}' AS DATETIME) AS update_datetime, '9999' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;""",
            """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('456' AS COL1, '4567' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1;""",
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_dont_merge_update_migrations_different_update_keys(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "123")],
                updates=[("COL2", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=None,
                filters=[("COL1", "456")],
                updates=[("COL3", "4567")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL1, '1234' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1;""",
            """UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL3 = updates.new__COL3
FROM (SELECT * FROM UNNEST([
    STRUCT('456' AS COL1, '4567' AS new__COL3)
])) updates
WHERE original.COL1 = updates.COL1;""",
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_dont_merge_update_migrations_different_filters_same_update_keys(
        self,
    ) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL2", "123")],
                updates=[("COL2", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "456")],
                updates=[("COL2", "4567")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('123' AS COL2, CAST('{_DATE_1.isoformat()}' AS DATETIME) AS update_datetime, '1234' AS new__COL2)
])) updates
WHERE original.COL2 = updates.COL2 AND original.update_datetime = updates.update_datetime;""",
            f"""UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('456' AS COL1, CAST('{_DATE_1.isoformat()}' AS DATETIME) AS update_datetime, '4567' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;""",
        ]

        self.assertEqual(expected_queries_map, queries_map)

    def test_dont_merge_delete_and_update_migrations(self) -> None:
        migrations_file = self._migration_file_path_for_tag("tagC")

        migrations: List[RawTableMigration] = [
            DeleteFromRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "1234")],
            ),
            UpdateRawTableMigration(
                migrations_file=migrations_file,
                update_datetime_filters=[_DATE_1],
                filters=[("COL1", "456")],
                updates=[("COL2", "4567")],
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            queries_map = RawTableMigrationGenerator.migration_queries(migrations)

        expected_queries_map = [
            f"""DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`
WHERE STRUCT(COL1, update_datetime) IN (
    STRUCT("1234", "{_DATE_1.isoformat()}")
);""",
            f"""UPDATE `recidiviz-456.us_xx_raw_data.tagC` original
SET COL2 = updates.new__COL2
FROM (SELECT * FROM UNNEST([
    STRUCT('456' AS COL1, CAST('{_DATE_1.isoformat()}' AS DATETIME) AS update_datetime, '4567' AS new__COL2)
])) updates
WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;""",
        ]

        self.assertEqual(expected_queries_map, queries_map)
