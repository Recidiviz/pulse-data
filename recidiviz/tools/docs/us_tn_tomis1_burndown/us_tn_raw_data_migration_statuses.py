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
"""Manually-maintained completion status for each US_TN raw data migration
(Tranche 1/Phase 1 of the TOMIS 1.0 -> TOMIS 2.0 migration, see TN-1939):
whether the data-quality issue each migrations_<file_tag>.py module corrects
has been confirmed to still exist (or not) in the corresponding MiCase data.
"""
from enum import Enum


class RawDataMigrationStatus(Enum):
    """Completion status of a single US_TN raw data migration, tracked by file
    tag in US_TN_RAW_DATA_MIGRATION_STATUSES.
    """

    # investigation needed to determine if we need to migrate the migrations for this file
    # tag
    NEEDS_INVESTIGATION = "NEEDS INVESTIGATION"
    # we determined that we need to migrate the migrations for this file tag, but the work
    # to actually migrate them is not yet complete
    REQUIRES_MIGRATION = "REQUIRES MIGRATION"
    # migration for this file tag is complete
    MIGRATED = "MIGRATED"
    # no migration is needed -- either it is fixed at the source data or no longer relevant
    NO_MIGRATION_NEEDED = "NO MIGRATION NEEDED"


US_TN_RAW_DATA_MIGRATION_STATUSES: dict[str, RawDataMigrationStatus] = {
    "AssignedStaff": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "Classification": RawDataMigrationStatus.MIGRATED,
    "Diversion": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "JOCharge": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "JOIdentification": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "JOMiscellaneous": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "JOSentence": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "JOSpecialConditions": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "OffenderMovement": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "OffenderName": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "Sentence": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "SentenceAction": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "SentenceMiscellaneous": RawDataMigrationStatus.NEEDS_INVESTIGATION,
    "StaffEmailByAlias": RawDataMigrationStatus.NO_MIGRATION_NEEDED,
    "SupervisionPlan": RawDataMigrationStatus.NEEDS_INVESTIGATION,
}
