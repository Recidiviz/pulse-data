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
from typing import Dict, Tuple, List, Optional

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import RawTableMigration, \
    RAW_TABLE_MIGRATION_FILE_PREFIX, MIGRATIONS_SUBDIR, RAW_DATA_SUBDIR

_MIGRATIONS_LIST_EXPECTED_NAME = 'MIGRATIONS'

RawFileKey = Tuple[str, datetime.datetime]


class DirectIngestRawTableMigrationCollector(ModuleCollectorMixin):
    """A class that finds all raw table migrations queries defined for a given region.
    Migrations must be defined in a top-level list of RawTableMigration named MIGRATIONS in a file with a path like
    this:

        /path/to/provided/regions_module/{region_code}/raw_data/migrations/migrations_{file_tag}.yaml

    Example:
        recidiviz/ingest/direct/regions/us_pa/raw_data/migrations/migrations_dbo_Miscon.yaml
    """

    def __init__(self, region_code: str, regions_module_override: Optional[ModuleType] = None):
        regions_module = regions_module_override or regions
        self.region_code = region_code
        self.migrations_dir_module = \
            self.get_relative_module(regions_module, [self.region_code.lower(), RAW_DATA_SUBDIR, MIGRATIONS_SUBDIR])

    def collect_raw_table_migration_queries(self) -> Dict[RawFileKey, List[str]]:
        """Finds all migrations defined for this region and returns a dict indexing all migration queries that should be
         run for a given raw file.
         """
        migrations = defaultdict(list)
        migrations_list = self.collect_raw_table_migrations()
        for migration in migrations_list:
            if not isinstance(migration, RawTableMigration):
                raise ValueError(f'Found unexpected migration type: {type(migration)}')
            for update_datetime, migration_query in migration.migration_queries_by_update_datetime().items():
                migrations[(migration.file_tag, update_datetime)].append(migration_query)

        return migrations

    def collect_raw_table_migrations(self) -> List[RawTableMigration]:
        """Finds all raw table migration objects defined for this region."""
        table_migrations_modules = self.get_submodules(self.migrations_dir_module, RAW_TABLE_MIGRATION_FILE_PREFIX)

        all_migrations = []
        for table_migrations_module in table_migrations_modules:
            if not hasattr(table_migrations_module, _MIGRATIONS_LIST_EXPECTED_NAME):
                raise ValueError(f'File [{table_migrations_module.__file__}] has no top-level attribute called '
                                 f'[{_MIGRATIONS_LIST_EXPECTED_NAME}]')

            migrations_list = getattr(table_migrations_module, _MIGRATIONS_LIST_EXPECTED_NAME)
            all_migrations.extend(migrations_list)
        return all_migrations
