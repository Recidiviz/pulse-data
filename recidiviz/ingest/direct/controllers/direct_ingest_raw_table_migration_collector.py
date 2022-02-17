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
from typing import Dict, List, Optional, Tuple

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import (
    MIGRATIONS_SUBDIR,
    RAW_DATA_SUBDIR,
    RAW_TABLE_MIGRATION_FILE_PREFIX,
    RawTableMigration,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration_generator import (
    RawTableMigrationGenerator,
)

_MIGRATIONS_FUNCTION_EXPECTED_NAME = "get_migrations"

RawFileKey = Tuple[str, Optional[datetime.datetime]]


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
        self, region_code: str, regions_module_override: Optional[ModuleType] = None
    ):
        regions_module = regions_module_override or regions
        self.region_code = region_code
        self.migrations_dir_module = self.get_relative_module(
            regions_module,
            [self.region_code.lower(), RAW_DATA_SUBDIR, MIGRATIONS_SUBDIR],
        )

    def collect_raw_table_migration_queries(
        self, sandbox_dataset_prefix: Optional[str]
    ) -> Dict[str, List[str]]:
        """Finds all migrations defined for this region and returns a dict indexing all migration queries that should be
        run for a given raw file.
        """
        migrations_list = self.collect_raw_table_migrations()
        migrations_by_file_tag = defaultdict(list)
        for migration in migrations_list:
            migrations_by_file_tag[migration.file_tag].append(migration)

        return {
            file_tag: RawTableMigrationGenerator.migration_queries(
                migrations, sandbox_dataset_prefix=sandbox_dataset_prefix
            )
            for file_tag, migrations in migrations_by_file_tag.items()
        }

    def collect_raw_table_migrations(self) -> List[RawTableMigration]:
        """Finds all raw table migration objects defined for this region."""
        table_migrations_modules = self.get_submodules(
            self.migrations_dir_module, RAW_TABLE_MIGRATION_FILE_PREFIX
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
