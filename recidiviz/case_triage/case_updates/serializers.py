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
"""Implements common serializers for different CaseUpdate subtypes."""
from typing import Dict

from recidiviz.case_triage.case_updates.types import (
    CaseActionVersionData,
    CaseUpdateActionType,
)
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient

assessment_mappings = {"last_recorded_date": "most_recent_assessment_date"}
contact_mappings = {"last_recorded_date": "most_recent_face_to_face_date"}
employment_mappings = {"last_employer": "employer"}
supervision_level_mappings = {"last_supervision_level": "supervision_level"}

_ACTION_TYPE_TO_MAPPINGS: Dict[CaseUpdateActionType, Dict[str, str]] = {
    # Risk assessment serializers
    CaseUpdateActionType.COMPLETED_ASSESSMENT: assessment_mappings,
    CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA: assessment_mappings,
    # Employment serializers
    CaseUpdateActionType.FOUND_EMPLOYMENT: employment_mappings,
    CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA: employment_mappings,
    # Face to face contact serializers
    CaseUpdateActionType.SCHEDULED_FACE_TO_FACE: contact_mappings,
    CaseUpdateActionType.INCORRECT_CONTACT_DATA: contact_mappings,
    # TODO(#5721): Figure out what additional metadata is needed
    CaseUpdateActionType.DISCHARGE_INITIATED: {},
    CaseUpdateActionType.DOWNGRADE_INITIATED: supervision_level_mappings,
    CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA: supervision_level_mappings,
    CaseUpdateActionType.NOT_ON_CASELOAD: {},
    CaseUpdateActionType.CURRENTLY_IN_CUSTODY: {},
    CaseUpdateActionType.INCORRECT_NEW_TO_CASELOAD_DATA: {
        "last_days_with_current_po": "days_with_current_po"
    },
}


def serialize_client_case_version(
    action: CaseUpdateActionType, client: ETLClient
) -> CaseActionVersionData:
    mappings = _ACTION_TYPE_TO_MAPPINGS[action]
    return CaseActionVersionData.from_client_mappings(mappings, client)
