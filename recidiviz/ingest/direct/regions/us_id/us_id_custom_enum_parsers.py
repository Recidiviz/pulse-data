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
import functools
from typing import Set

from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactMethod,
)
from recidiviz.common.text_analysis import (
    TextAnalyzer,
    TextEntity,
    TextMatchingConfiguration,
)
from recidiviz.ingest.direct.regions.us_id.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)

TEXT_ANALYZER = TextAnalyzer(
    configuration=TextMatchingConfiguration(
        stop_words_to_remove={"in", "out"},
        text_entities=[UsIdTextEntity.ANY_TREATMENT, UsIdTextEntity.TREATMENT_COMPLETE],
    )
)


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
    return StateProgramAssignmentParticipationStatus.EXTERNAL_UNKNOWN


def discharge_reason_from_agnt_note_title(
    raw_text: str,
) -> StateProgramAssignmentDischargeReason:
    """Sets the discharge reason only if treatment completion is matched as an entity."""
    matched_entities = _match_note_title(raw_text)
    return (
        StateProgramAssignmentDischargeReason.COMPLETED
        if UsIdTextEntity.ANY_TREATMENT in matched_entities
        and UsIdTextEntity.TREATMENT_COMPLETE in matched_entities
        else StateProgramAssignmentDischargeReason.EXTERNAL_UNKNOWN
    )


@functools.lru_cache(maxsize=128)
def _match_note_title(agnt_note_title: str) -> Set[TextEntity]:
    """Returns the entities that the agnt_note_title matches to."""
    return TEXT_ANALYZER.extract_entities(agnt_note_title)


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
