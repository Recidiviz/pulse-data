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
"""Custom enum parsers functions for US_ID. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_id_custom_enum_parsers.<function name>
"""
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactMethod,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.ingest.direct.regions.us_id.us_id_custom_parsers import _match_note_title
from recidiviz.ingest.direct.regions.us_id.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)


# Custom Enum parsers needed for us_id_agnt_case_updt
def participation_status_from_agnt_note_title(
    raw_text: str,
) -> StateProgramAssignmentParticipationStatus:
    """Sets the participation based on the treatment entities matched."""
    matched_entities = _match_note_title(raw_text)
    if (
        UsIdTextEntity.ANY_TREATMENT in matched_entities
        and UsIdTextEntity.TREATMENT_COMPLETE in matched_entities
    ):
        return StateProgramAssignmentParticipationStatus.DISCHARGED
    if UsIdTextEntity.ANY_TREATMENT in matched_entities:
        return StateProgramAssignmentParticipationStatus.IN_PROGRESS
    raise ValueError(
        f"Unexpected matched entities: {matched_entities} for StateProgramAssignmentParticipationStatus"
    )


def violation_response_decision_from_agnt_note_title(
    raw_text: str,
) -> StateSupervisionViolationResponseDecision:
    """Sets the violation response decision based on certain entity matches."""
    matched_entities = _match_note_title(raw_text)
    if UsIdTextEntity.REVOCATION_INCLUDE in matched_entities:
        raise ValueError("Unexpected received REVOCATION_INCLUDE in matched entities")
    if UsIdTextEntity.AGENTS_WARNING in matched_entities:
        return StateSupervisionViolationResponseDecision.WARNING
    if (
        UsIdTextEntity.REVOCATION in matched_entities
        and UsIdTextEntity.REVOCATION_INCLUDE not in matched_entities
    ):
        return StateSupervisionViolationResponseDecision.REVOCATION
    raise ValueError(
        f"Unexpected matched entities: {matched_entities} for StateSupervisionViolationResponseDecision"
    )


# Custom Enum Parsers needed for us_id_sprvsn_cntc_v3
def contact_method_from_contact_fields(raw_text: str) -> StateSupervisionContactMethod:
    location_text, type_text = raw_text.split("##")
    if location_text == "TELEPHONE":
        return StateSupervisionContactMethod.TELEPHONE
    if location_text in ("MAIL", "EMAIL", "FAX"):
        return StateSupervisionContactMethod.WRITTEN_MESSAGE
    if type_text == "VIRTUAL":
        return StateSupervisionContactMethod.VIRTUAL
    if type_text == "WRITTEN CORRESPONDENCE":
        return StateSupervisionContactMethod.WRITTEN_MESSAGE
    if location_text != "NONE":
        return StateSupervisionContactMethod.IN_PERSON
    if type_text == "NEGATIVE CONTACT":
        return StateSupervisionContactMethod.IN_PERSON
    return StateSupervisionContactMethod.INTERNAL_UNKNOWN
