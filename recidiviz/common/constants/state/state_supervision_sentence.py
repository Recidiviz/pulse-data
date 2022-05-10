# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Shared constants related to supervision."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionSentenceSupervisionType(StateEntityEnum):
    COMMUNITY_CORRECTIONS = (
        state_enum_strings.state_supervision_sentence_supervision_type_community_corrections
    )
    PAROLE = state_enum_strings.state_supervision_sentence_supervision_type_parole
    PROBATION = state_enum_strings.state_supervision_sentence_supervision_type_probation
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionSentenceSupervisionType"]:
        return _SUPERVISION_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of supervision associated with a sentence."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_SENTENCE_SUPERVISION_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_SENTENCE_SUPERVISION_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS: "Used when a person "
    "has been sentenced by the court to community-based supervision and/or treatment "
    "services.",
    StateSupervisionSentenceSupervisionType.PAROLE: "Parole is the type "
    "of supervision where someone is serving the remaining portion of an incarceration "
    "sentence in the community. The person’s release from prison is conditional on "
    "them following certain supervision requirements as determined by the parole "
    "board and the person’s supervision officer. All periods of time spent on parole "
    "are legally associated with a sentence to incarceration. The presence of this "
    "value in our schema is an artifact of the way that we currently structure "
    "sentencing data for some states, which we have plans to change (TODO(#10389)).",
    StateSupervisionSentenceSupervisionType.PROBATION: "Used when a person has been "
    "sentenced by the court to a period of supervision - often in lieu of being "
    "sentenced to incarceration. Individuals on probation report to a supervision "
    "officer, and must follow the conditions of their supervision as determined by "
    "the judge and person’s supervision officer.",
}

# The type of supervision associated with a sentence.

_SUPERVISION_TYPE_MAP = {
    "COMMUNITY CORRECTIONS": StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS,
    "EXTERNAL UNKNOWN": StateSupervisionSentenceSupervisionType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN,
    "PAROLE": StateSupervisionSentenceSupervisionType.PAROLE,
    "PROBATION": StateSupervisionSentenceSupervisionType.PROBATION,
}
