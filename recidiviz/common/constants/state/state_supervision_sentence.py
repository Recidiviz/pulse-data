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
    CIVIL_COMMITMENT = (
        state_enum_strings.state_supervision_sentence_supervision_type_civil_commitment
    )
    # Sentenced to community based supervision and treatment services.
    COMMUNITY_CORRECTIONS = (
        state_enum_strings.state_supervision_sentence_supervision_type_community_corrections
    )
    HALFWAY_HOUSE = (
        state_enum_strings.state_supervision_sentence_supervision_type_halfway_house
    )
    PAROLE = state_enum_strings.state_supervision_sentence_supervision_type_parole
    POST_CONFINEMENT = (
        state_enum_strings.state_supervision_sentence_supervision_type_post_confinement
    )
    PRE_CONFINEMENT = (
        state_enum_strings.state_supervision_sentence_supervision_type_pre_confinement
    )
    PROBATION = state_enum_strings.state_supervision_sentence_supervision_type_probation
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionSentenceSupervisionType"]:
        return _SUPERVISION_TYPE_MAP


_SUPERVISION_TYPE_MAP = {
    "CIVIL COMMITMENT": StateSupervisionSentenceSupervisionType.CIVIL_COMMITMENT,
    "COMMUNITY CORRECTIONS": StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS,
    "CC": StateSupervisionSentenceSupervisionType.CIVIL_COMMITMENT,
    "EXTERNAL UNKNOWN": StateSupervisionSentenceSupervisionType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN,
    "HALFWAY HOUSE": StateSupervisionSentenceSupervisionType.HALFWAY_HOUSE,
    "HALFWAY HOME": StateSupervisionSentenceSupervisionType.HALFWAY_HOUSE,
    "HALFWAY": StateSupervisionSentenceSupervisionType.HALFWAY_HOUSE,
    "PAROLE": StateSupervisionSentenceSupervisionType.PAROLE,
    "POST CONFINEMENT": StateSupervisionSentenceSupervisionType.POST_CONFINEMENT,
    "POST RELEASE": StateSupervisionSentenceSupervisionType.POST_CONFINEMENT,
    "PRE CONFINEMENT": StateSupervisionSentenceSupervisionType.PRE_CONFINEMENT,
    "PRE RELEASE": StateSupervisionSentenceSupervisionType.PRE_CONFINEMENT,
    "PROBATION": StateSupervisionSentenceSupervisionType.PROBATION,
}
