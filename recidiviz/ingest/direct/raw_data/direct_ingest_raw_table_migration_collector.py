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
"""Defines a class that finds all raw table migrations defined for a given region."""

import datetime
from collections import defaultdict
from types import ModuleType
from typing import Dict, List, Optional

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration import (
    MIGRATIONS_SUBDIR,
    RAW_DATA_SUBDIR,
    RAW_TABLE_MIGRATION_FILE_PREFIX,
    RawTableMigration,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_generator import (
    RawTableMigrationGenerator,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_MIGRATIONS_FUNCTION_EXPECTED_NAME = "get_migrations"


class DirectIngestRawTableMigrationCollector(ModuleCollectorMixin):
    """A class that finds all raw table migrations queries defined for a given region.

    Migrations must be defined in a top-level function named get_migrations that takes
    no parameters and produces a list of RawTableMigration. These must be defined in a
    file with a path like this:

        /path/to/provided/regions_module/{region_code}/raw_data/migrations/migrations_{file_tag}.py

    Example:
        recidiviz/ingest/direct/regions/us_pa/raw_data/migrations/migrations_dbo_Miscon.py
    """

    def __init__(
        self,
        region_code: str,
        instance: DirectIngestInstance,
        regions_module_override: Optional[ModuleType] = None,
    ) -> None:
        self.regions_module = regions_module_override or regions
        self.region_code = region_code
        self.instance = instance
        self._migration_by_file_tag: Dict[str, List[RawTableMigration]] = {}

    def get_raw_table_migration_queries_for_file_tag(
        self,
        file_tag: str,
        raw_table_address: BigQueryAddress,
        data_update_datetime: Optional[datetime.datetime],
    ) -> List[str]:
        """Generates a migration query that will modify raw data for the raw data table
        specified by |file_tag| that is located at |raw_table_address|, optionally
        filtering migrations by |data_update_datetime|
        """
        # TODO(#34696) Enforce timezone awareness for data_update_datetime
        if data_update_datetime is not None and data_update_datetime.tzinfo is not None:
            raise ValueError(
                "data_update_datetime must be a naive datetime object, not a timezone-aware datetime object"
            )
        return RawTableMigrationGenerator.migration_queries(
            self.get_raw_table_migrations_by_file_tag()[file_tag],
            raw_table_address=raw_table_address,
            data_update_datetime=data_update_datetime,
        )

    def get_raw_table_migrations_by_file_tag(
        self,
    ) -> Dict[str, List[RawTableMigration]]:
        """Returns cached migrations by file tag, if it has been loaded; if not,
        descends and collects them.
        """
        if self._migration_by_file_tag:
            return self._migration_by_file_tag

        self._migration_by_file_tag = self.collect_raw_table_migrations_by_file_tag()
        return self._migration_by_file_tag

    def collect_raw_table_migrations_by_file_tag(
        self,
    ) -> Dict[str, List[RawTableMigration]]:
        """Collects raw table migrations by file_tag"""
        migration_by_file_tag = defaultdict(list)
        for migration in self.collect_raw_table_migrations():
            migration_by_file_tag[migration.file_tag].append(migration)

        return migration_by_file_tag

    def collect_raw_table_migrations(self) -> List[RawTableMigration]:
        """Finds all raw table migration objects defined for this region."""
        migrations_dir_module = self.get_relative_module(
            self.regions_module,
            [self.region_code.lower(), RAW_DATA_SUBDIR, MIGRATIONS_SUBDIR],
        )
        table_migrations_modules = self.get_submodules(
            migrations_dir_module, RAW_TABLE_MIGRATION_FILE_PREFIX
        )

        all_migrations = []
        for table_migrations_module in table_migrations_modules:
            if not hasattr(table_migrations_module, _MIGRATIONS_FUNCTION_EXPECTED_NAME):
                raise ValueError(
                    f"File [{table_migrations_module.__file__}] has no top-level "
                    f"function [{_MIGRATIONS_FUNCTION_EXPECTED_NAME}]"
                )
            migrations_list = getattr(
                table_migrations_module, _MIGRATIONS_FUNCTION_EXPECTED_NAME
            )()
            all_migrations.extend(migrations_list)
        return all_migrations
