# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared reporting helpers for the manually-tracked raw data migrations table
in the US_TN TOMIS 1.0 -> TOMIS 2.0 (MiCase) migration burndown. See TN-1939.

Computes, from the live US_TN raw table migration registry, which file tags
have a raw_data/migrations/migrations_<file_tag>.py module defined today --
consumed by us_tn_tomis1_burndown_markdown_generator.py.
"""
from functools import cache

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_tomis_migration_reporting import (
    raise_if_names_untracked_or_stale,
)


@cache
def us_tn_raw_table_migration_collector() -> DirectIngestRawTableMigrationCollector:
    # Migration collection does not vary by instance -- PRIMARY is arbitrary.
    return DirectIngestRawTableMigrationCollector(
        StateCode.US_TN.value.lower(), instance=DirectIngestInstance.PRIMARY
    )


def us_tn_real_raw_data_migration_file_tags() -> frozenset[str]:
    """Returns the file tags of every real, currently-defined US_TN raw table
    migration module.
    """
    return frozenset(
        us_tn_raw_table_migration_collector().collect_raw_table_migrations_by_file_tag()
    )


def validate_raw_data_migration_statuses(statuses: dict[str, str]) -> None:
    """Raises ValueError unless `statuses` classifies every real US_TN raw
    data migration file tag (see us_tn_raw_data_migration_statuses.py): every
    real file tag with a migrations module must appear as a key, and every
    key must correspond to a real, currently-existing migrations module. Does
    not prescribe what status to assign -- that judgment call (has the
    underlying data issue been confirmed to persist in MiCase data?) is made
    by whoever reviews the migration.
    """
    raise_if_names_untracked_or_stale(
        real_names=us_tn_real_raw_data_migration_file_tags(),
        tracked_names=set(statuses),
        untracked_error_prefix=(
            "Found US_TN raw data migrations not tracked in "
            "US_TN_RAW_DATA_MIGRATION_STATUSES (us_tn_raw_data_migration_statuses.py). "
            "Add a status for each"
        ),
        stale_error_prefix=(
            "Found entries in US_TN_RAW_DATA_MIGRATION_STATUSES with no "
            "corresponding real US_TN raw data migration. Remove these"
        ),
    )
