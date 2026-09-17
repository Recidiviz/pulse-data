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
"""Manually-maintained pairing of US_TN ingest views (Tranche 1 of the TOMIS
1.0 -> TOMIS 2.0 migration, see TN-1939) from their legacy TOMIS 1.0 name to
their TOMIS 2.0 (MiCase) replacement, once one has been written.

This dict is NOT auto-populated. When a new US_TN ingest view is added, the
completeness check in
us_tn_ingest_view_migration_reporting.validate_ingest_view_migration_pairs
will fail until it is added here -- either as a new key (if it has no
replacement yet) or as the value of the existing key it replaces. The check
does not prescribe which; that judgment call belongs to whoever adds the view.
"""

# The entries below are scaffolded with a placeholder value
# of None -- add the 2.0 ingest view as they are written
US_TN_INGEST_VIEW_MIGRATION_PAIRS: dict[str, str | None] = {
    "ProgramAssignment": None,
    "AssignedStaff": None,
    "AssignedStaffSupervisionPeriod_v2": None,
    "CAFScoreAssessment": None,
    "DisciplinaryIncarcerationIncident": "DisciplinaryIncarcerationIncident_v2",
    "InferredViolations": None,
    "OffenderMovementIncarcerationPeriod_v3": None,
    "OffenderName": "OffenderName_v2",
    "RCAFandDCAFAssessments": None,
    "STGAssessment": None,
    "Staff": None,
    "StaffCaseloadTypePeriod": None,
    "StaffRoleLocationPeriods": None,
    "StaffSupervisorPeriods": None,
    "VantagePointAssessments": None,
    "ViolationsAndSanctions": None,
    "diversion_sentence": None,
    "diversion_sentence_length": None,
    "drug_screen": None,
    "isc_sentence": None,
    "isc_sentence_length": None,
    "sentence_and_charge": None,
    "sentence_length": None,
}
