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
US_TN_RAW_DATA_MIGRATION_STATUSES: dict[str, str] = {
    "AssignedStaff": "NEEDS INVESTIGATION",
    "Classification": "NEEDS INVESTIGATION",
    "Diversion": "NEEDS INVESTIGATION",
    "JOCharge": "NEEDS INVESTIGATION",
    "JOIdentification": "NEEDS INVESTIGATION",
    "JOMiscellaneous": "NEEDS INVESTIGATION",
    "JOSentence": "NEEDS INVESTIGATION",
    "JOSpecialConditions": "NEEDS INVESTIGATION",
    "OffenderMovement": "NEEDS INVESTIGATION",
    "OffenderName": "NEEDS INVESTIGATION",
    "Sentence": "NEEDS INVESTIGATION",
    "SentenceAction": "NEEDS INVESTIGATION",
    "SentenceMiscellaneous": "NEEDS INVESTIGATION",
    "StaffEmailByAlias": "NEEDS INVESTIGATION",
    "SupervisionPlan": "NEEDS INVESTIGATION",
}
