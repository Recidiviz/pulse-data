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
"""Utility classes for defining migrations on our direct ingest raw data tables."""

import datetime
import os
import re
from typing import List, Optional, Tuple

from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.utils import metadata


RAW_DATA_SUBDIR = "raw_data"
MIGRATIONS_SUBDIR = "migrations"
RAW_TABLE_MIGRATION_FILE_PREFIX = "migrations_"


class RawTableMigration:
    """Base class for generating migration queries for a given raw data table."""

    def __init__(
        self,
        migrations_file: str,
        filters: List[Tuple[str, str]],
        update_datetime_filters: Optional[List[datetime.datetime]],
    ):
        """
        Args:
            migrations_file: The path of the file where this migration is defined, used to derive the raw table region
                and file tag. Expected to end with /{region_code}/raw_data/migrations/migrations_{file_tag}.py.
            filters: List of (column name, value) tuples that will be translated to filter statements in the
                WHERE clause of the migration query.
            update_datetime_filters: Optional list of datetimes to filter which update_datetime values the migration
                should be run on. If null, the migration will be run on all versions of the migrations_file.
        """
        if update_datetime_filters and any(
            dt is None for dt in update_datetime_filters
        ):
            raise ValueError(
                "Found None value in update_datetime_filters. To have a migration that "
                "will run on all file versions, pass in update_datetime_filters=None."
            )

        filter_keys = [f[0] for f in filters]
        if len(set(filter_keys)) != len(filter_keys):
            raise ValueError(f"Found duplicate filter keys: {filter_keys}")

        self.update_datetime_filters = update_datetime_filters
        self.file_tag = self._file_tag_from_migrations_file(migrations_file)
        self.filters = dict(filters)
        self._region_code_lower = self._region_code_lower_from_migrations_file(
            migrations_file
        )

        self._validate_column_value_list("filters", filters)

    def raw_table(self, sandbox_dataset_prefix: Optional[str]) -> str:
        """Returns the BQ raw table for this migration. Must be calculated dynamically rather than in the constructor
        because these migrations can be defined as top-level vars where the project_id is not yet available.
        """
        dataset_id = raw_tables_dataset_for_region(
            region_code=self._region_code_lower,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )
        return f"{metadata.project_id()}.{dataset_id}.{self.file_tag}"

    @property
    def ordered_filter_keys(self) -> Tuple[str, ...]:
        """A sorted tuple of all filter columns used to identify the record(s) for this
        migration.
        """
        filter_keys = sorted(self.filters.keys())
        if self.update_datetime_filters:
            filter_keys.append(UPDATE_DATETIME_COL_NAME)

        return tuple(filter_keys)

    @property
    def ordered_filter_values(self) -> List[Tuple[str, ...]]:
        """A list of sets filter values used to identify the record(s) for this
        migration. The values are sorted in the same order as the ordered_filter_keys.
        """
        if not self.update_datetime_filters:
            return [
                tuple(
                    self.filters[filter_key] for filter_key in self.ordered_filter_keys
                )
            ]

        result = []
        for dt in self.update_datetime_filters:
            values = []
            for filter_key in self.ordered_filter_keys:
                if filter_key == UPDATE_DATETIME_COL_NAME:
                    values.append(dt.isoformat())
                else:
                    values.append(self.filters[filter_key])
            result.append(tuple(values))
        return result

    @staticmethod
    def _validate_column_value_list(list_name: str, col_value_list: List) -> None:
        if not isinstance(col_value_list, list):
            raise ValueError(
                f"Unexpected value type [{type(col_value_list)}] for [{list_name}]."
            )

        column_names = {col for col, _value in col_value_list}

        if len(column_names) < len(col_value_list):
            raise ValueError(
                f"Found duplicated columns in [{list_name}]: {col_value_list}"
            )

    @staticmethod
    def _file_tag_from_migrations_file(migrations_file: str) -> str:
        file_name, _ext = os.path.splitext(os.path.basename(migrations_file))
        if not file_name.startswith(RAW_TABLE_MIGRATION_FILE_PREFIX):
            raise ValueError(f"Unexpected migrations file name [{file_name}]")
        file_tag = file_name[len(RAW_TABLE_MIGRATION_FILE_PREFIX) :]
        return file_tag

    @staticmethod
    def _region_code_lower_from_migrations_file(migrations_file: str) -> str:
        path_parts = os.path.normpath(migrations_file).split(os.sep)
        region_code, raw_data_dir, migrations_dir_name, _file_name = path_parts[-4:]
        if (raw_data_dir, migrations_dir_name) != (RAW_DATA_SUBDIR, MIGRATIONS_SUBDIR):
            raise ValueError(
                f"Unexpected path parts: {(raw_data_dir, migrations_dir_name)}"
            )
        if not re.match(r"us_[a-z]{2}(_[a-z_]+)?", region_code):
            raise ValueError(f"Unexpected region code: {region_code}")
        return region_code


class UpdateRawTableMigration(RawTableMigration):
    """Class containing information about an UPDATE migration for one or more similar
    records in a raw data table.
    """

    def __init__(
        self,
        migrations_file: str,
        filters: List[Tuple[str, str]],
        updates: List[Tuple[str, Optional[str]]],
        update_datetime_filters: Optional[List[datetime.datetime]],
    ):
        super().__init__(migrations_file, filters, update_datetime_filters)
        self._validate_column_value_list("updates", updates)
        self.updates = dict(updates)

    @property
    def ordered_update_keys(self) -> Tuple[str, ...]:
        """A sorted tuple of all columns that will be updated in this migration."""
        return tuple(sorted(self.updates.keys()))


class DeleteFromRawTableMigration(RawTableMigration):
    """Class containing information about a DELETE FROM migration for one or more
    similar records in a raw data table.
    """
