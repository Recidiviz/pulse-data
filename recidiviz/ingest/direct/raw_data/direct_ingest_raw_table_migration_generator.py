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
"""Provides functionality for generating a set of migration queries for migrations
on a given raw table."""
import datetime
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple, Type, cast

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration import (
    DeleteFromRawTableMigration,
    RawTableMigration,
    UpdateRawTableMigration,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

DELETE_FROM_MIGRATION_TEMPLATE = """DELETE FROM `{raw_table}`
WHERE {filter_clause};"""

UPDATE_MIGRATION_TEMPLATE = """UPDATE `{raw_table}` original
SET {update_clause}
FROM ({update_table_clause}) updates
WHERE {filter_clause};"""


class RawTableMigrationGenerator:
    """Provides functionality for generating a set of migration queries for migrations
    on a given raw table."""

    @classmethod
    def migration_queries(
        cls,
        migrations: List["RawTableMigration"],
        raw_table_address: BigQueryAddress,
        data_update_datetime: Optional[datetime.datetime],
    ) -> List[str]:
        """Generates a list of migration query strings from |migrations| that can be
        applied to |raw_table_address| in Big Query. Merges all similarly shaped record
        migrations into the same query where possible.

        Throws if the migration configs are not all for migrations on the same raw
        table.

        If provided, |data_update_datetime| indicates that the data we will run these
        migration queries against has a single associated update_datetime. This allows
        us to optimize which migration queries are run (some migrations apply only to
        data with certain update_datetime values).
        """
        # TODO(#34696) Enforce timezone awareness for data_update_datetime
        if data_update_datetime is not None and data_update_datetime.tzinfo is not None:
            raise ValueError(
                "data_update_datetime must be a naive datetime object, not a timezone-aware datetime object"
            )

        cls._check_file_tags_match(migrations)

        raw_table = raw_table_address.to_project_specific_address(
            metadata.project_id()
        ).to_str()

        groupings: Dict[
            Type[RawTableMigration], Dict[Tuple[str, ...], List[RawTableMigration]]
        ] = defaultdict(lambda: defaultdict(list))
        for migration in migrations:
            groupings[type(migration)][migration.ordered_filter_keys].append(migration)

        result = []
        for migration_cls, filter_key_to_migrations in groupings.items():
            for grouped_migrations in filter_key_to_migrations.values():
                filtered_grouped_migrations = (
                    cls._filter_migrations_for_data_update_datetime(
                        data_update_datetime, grouped_migrations
                    )
                )

                if not filtered_grouped_migrations:
                    continue

                if migration_cls == DeleteFromRawTableMigration:
                    delete_migrations = cast(
                        List[DeleteFromRawTableMigration], filtered_grouped_migrations
                    )
                    queries = cls._queries_for_delete_migrations(
                        delete_migrations,
                        raw_table=raw_table,
                    )
                elif migration_cls == UpdateRawTableMigration:
                    update_migrations = cast(
                        List[UpdateRawTableMigration], filtered_grouped_migrations
                    )
                    queries = cls._queries_for_update_migrations(
                        update_migrations,
                        raw_table=raw_table,
                    )
                else:
                    raise ValueError(
                        f"Unexpected migration class [{migration_cls.__name__}]"
                    )

                result.extend(queries)
        return result

    @staticmethod
    def _filter_migrations_for_data_update_datetime(
        data_update_datetime: Optional[datetime.datetime],
        migrations_shared_filter_keys: List[RawTableMigration],
    ) -> List[RawTableMigration]:
        """Returns all migrations in |migrations_shared_filter_keys| that dont have
            -  an update_datetime_filter or
            -  |data_update_datetime| is included as a filter value value for in the
            migration's update_datetime filter values

        If that is the case, we can safely assume that this migration query
        would be a no-oop when running against a table that only contains data with
        update_datime of |update_datetime|.
        """
        if not data_update_datetime:
            return migrations_shared_filter_keys

        filtered_migrations = [
            migration
            for migration in migrations_shared_filter_keys
            if not migration.update_datetime_filters
            or data_update_datetime in migration.update_datetime_filters
        ]

        if len(filtered_migrations) != len(migrations_shared_filter_keys):
            logging.info(
                "Excluding [%s of %s] migrations as they filter by an update_datetime "
                "that is not included on the destination table",
                len(migrations_shared_filter_keys) - len(filtered_migrations),
                len(migrations_shared_filter_keys),
            )

        return filtered_migrations

    @classmethod
    def print_list(
        cls,
        migrations: List["RawTableMigration"],
        raw_table_addresses: Optional[List[BigQueryAddress]] = None,
    ) -> None:
        """Prints out all queries generated by the migration objects in this list. For
        use in debugging and to generate migrations that need to be run on data that has
        already been imported to BQ.
        """

        raw_tables: List[BigQueryAddress] = []

        if not raw_table_addresses:
            for ingest_instance in DirectIngestInstance:
                raw_tables.append(
                    BigQueryAddress(
                        dataset_id=raw_tables_dataset_for_region(
                            state_code=StateCode(
                                migrations[0].region_code_lower.upper()
                            ),
                            instance=ingest_instance,
                        ),
                        table_id=migrations[0].file_tag,
                    )
                )
        else:
            raw_tables = raw_table_addresses

        for raw_table in raw_tables:
            print(
                f"/************************ (project=[{metadata.project_id()}], "
                f"dataset=[{raw_table.dataset_id}], table=[{raw_table.table_id}]) "
                f"************************/"
            )

            for query in cls.migration_queries(
                migrations, raw_table, data_update_datetime=None
            ):
                print(query)

    @classmethod
    def _check_file_tags_match(cls, migrations: Sequence["RawTableMigration"]) -> None:
        """Throws if all migration objects do not have the same file tag."""
        file_tags = set(m.file_tag for m in migrations)
        if len(file_tags) > 1:
            raise ValueError(
                f"Found multiple file tags in migrations list, "
                f"expected only one: [{file_tags}]"
            )

    @classmethod
    def _check_filter_keys_match(
        cls, migrations: Sequence["RawTableMigration"]
    ) -> None:
        """Throws if all migration objects do not share the same set of filter columns."""
        cls._check_file_tags_match(migrations)
        ordered_filter_keys_set = set(m.ordered_filter_keys for m in migrations)
        if len(ordered_filter_keys_set) > 1:
            raise ValueError(
                f"Found multiple ordered_filter_keys in migrations list, "
                f"expected only one: [{ordered_filter_keys_set}]"
            )

    @classmethod
    def _check_update_keys_match(
        cls, migrations: List["UpdateRawTableMigration"]
    ) -> None:
        """Throws if all migration objects do not share the same set of update columns."""
        ordered_update_keys_set = set(m.ordered_update_keys for m in migrations)
        if len(ordered_update_keys_set) > 1:
            raise ValueError(
                f"Found multiple ordered_update_keys in migrations list, "
                f"expected only one: [{ordered_update_keys_set}]"
            )

    @classmethod
    def _queries_for_delete_migrations(
        cls,
        migrations: List["DeleteFromRawTableMigration"],
        raw_table: str,
    ) -> List[str]:
        """Generates a list of migration queries from a list of DELETE FROM migration
        config objects."""
        cls._check_filter_keys_match(migrations)
        ordered_filter_keys = migrations[0].ordered_filter_keys

        filter_names_str = ", ".join(filter_key for filter_key in ordered_filter_keys)

        filter_structs = []
        for m in migrations:
            for filter_values in m.ordered_filter_values:
                escaped_filter_values = [
                    filter_value.replace('"', '\\"') for filter_value in filter_values
                ]

                filter_values_str = ", ".join(
                    [f'"{filter_value}"' for filter_value in escaped_filter_values]
                )
                filter_structs.append(f"STRUCT({filter_values_str})")
        filter_structs_str = ",\n    ".join(filter_structs)

        filter_clause = f"STRUCT({filter_names_str}) IN (\n    {filter_structs_str}\n)"

        return [
            StrictStringFormatter().format(
                DELETE_FROM_MIGRATION_TEMPLATE,
                raw_table=raw_table,
                filter_clause=filter_clause,
            )
        ]

    @classmethod
    def _queries_for_update_migrations(
        cls,
        migrations: List["UpdateRawTableMigration"],
        raw_table: str,
    ) -> List[str]:
        """Generates a list of migration queries from a list of UPDATE migration
        config objects."""
        cls._check_filter_keys_match(migrations)

        migrations_by_update_keys = defaultdict(list)
        for migration in migrations:
            migrations_by_update_keys[migration.ordered_update_keys].append(migration)

        result = []
        for grouped_migrations in migrations_by_update_keys.values():
            result.append(
                cls._migration_query_for_migrations_matching_update_keys(
                    grouped_migrations,
                    raw_table=raw_table,
                )
            )

        return result

    @classmethod
    def _migration_query_for_migrations_matching_update_keys(
        cls,
        migrations: List["UpdateRawTableMigration"],
        raw_table: str,
    ) -> str:
        """Generates a list of migration queries from a list of UPDATE migration
        config objects that update the same set of columns."""
        cls._check_update_keys_match(migrations)
        ordered_filter_keys = migrations[0].ordered_filter_keys
        ordered_update_key = migrations[0].ordered_update_keys

        filter_clause = " AND ".join(
            [
                f"original.{filter_key} = updates.{filter_key}"
                for filter_key in ordered_filter_keys
            ]
        )

        update_clause = ", ".join(
            [
                f"{filter_key} = updates.new__{filter_key}"
                for filter_key in ordered_update_key
            ]
        )

        migration_update_structs = []
        for migration in migrations:
            for filters_value in migration.ordered_filter_values:
                struct_values = []
                for filter_key, filter_value in zip(
                    migration.ordered_filter_keys, filters_value
                ):
                    if filter_key == UPDATE_DATETIME_COL_NAME:
                        struct_values.append(
                            f"CAST('{filter_value}' AS DATETIME) AS {filter_key}"
                        )
                    else:
                        # Escape single quotes
                        filter_value = filter_value.replace("'", "\\'")
                        struct_values.append(f"'{filter_value}' AS {filter_key}")

                for update_key in migration.ordered_update_keys:
                    new_value = migration.updates[update_key]
                    if new_value:
                        new_value = new_value.replace("'", "\\'")
                        struct_values.append(f"'{new_value}' AS new__{update_key}")
                    else:
                        struct_values.append(
                            f"CAST(NULL AS STRING) AS new__{update_key}"
                        )
                struct_values_str = ", ".join(struct_values)
                migration_update_structs.append(f"STRUCT({struct_values_str})")

        migration_update_structs_str = ",\n    ".join(migration_update_structs)
        update_table_clause = (
            f"SELECT * FROM UNNEST([\n    {migration_update_structs_str}\n])"
        )

        return StrictStringFormatter().format(
            UPDATE_MIGRATION_TEMPLATE,
            raw_table=raw_table,
            update_clause=update_clause,
            update_table_clause=update_table_clause,
            filter_clause=filter_clause,
        )
