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

"""Constants related to a state Sentence."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSentenceStatus(StateEntityEnum):
    COMMUTED = state_enum_strings.state_sentence_status_commuted
    COMPLETED = state_enum_strings.state_sentence_status_completed
    PARDONED = state_enum_strings.state_sentence_status_pardoned
    PENDING = state_enum_strings.state_sentence_status_pending
    REVOKED = state_enum_strings.state_sentence_status_revoked
    SANCTIONED = state_enum_strings.state_sentence_status_sanctioned
    SERVING = state_enum_strings.state_sentence_status_serving
    SUSPENDED = state_enum_strings.state_sentence_status_suspended
    VACATED = state_enum_strings.state_sentence_status_vacated
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSentenceStatus"]:
        return _STATE_SENTENCE_STATUS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a sentence."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SENTENCE_STATUS_VALUE_DESCRIPTIONS


_STATE_SENTENCE_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSentenceStatus.COMMUTED: "Describes a sentence that has been commuted. "
    "“Commutation” is a reduction of a sentence to a lesser period of time. This is "
    "different than `PARDONED` because the conviction has not been cleared from the "
    "person’s record.",
    StateSentenceStatus.COMPLETED: "Used when a person has served the entirety of the "
    "sentence.",
    StateSentenceStatus.PARDONED: "Describes a sentence associated with a conviction "
    "that has been pardoned. When a person is pardoned, there is immediate release "
    "from any active form of incarceration or supervision related to the pardoned "
    "conviction. This is different from `COMMUTED` because the person’s conviction is "
    "completely cleared when they are pardoned. This is distinct from `VACATED`, "
    "because the conviction is still legally valid, it has just been forgiven.",
    StateSentenceStatus.PENDING: "Describes a sentence for which the associated trial "
    "is still in progress.",
    StateSentenceStatus.REVOKED: "Used when a person’s supervision has been revoked "
    "and they are consequently re-sentenced.",
    StateSentenceStatus.SANCTIONED: "Used on a sentence that does not include any "
    "incarceration or supervision requirement, but may include, for example, fines "
    "and fees.",
    StateSentenceStatus.SERVING: "Describes a sentence that is actively being "
    "served by the person.",
    StateSentenceStatus.SUSPENDED: "Describes a sentence that has has not yet been "
    "enacted. This usually occurs when the execution of a sentence has been "
    "conditionally deferred on the person completing some type of supervision or "
    "diversionary programming.",
    StateSentenceStatus.VACATED: "Describes a sentence that has been vacated, "
    "which happens when the legal judgment on a conviction has become legally void, a "
    "conviction has been overturned, or a case as been dismissed. When a sentence is "
    "vacated, there is immediate release from any active form of incarceration or "
    "supervision related to the vacated conviction. This is distinct from `PARDONED`, "
    "because the sentence was cleared as a result of it being deemed legally void.",
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_SENTENCE_STATUS_MAP = {
    "COMMUTED": StateSentenceStatus.COMMUTED,
    "COMPLETED": StateSentenceStatus.COMPLETED,
    "EXTERNAL UNKNOWN": StateSentenceStatus.EXTERNAL_UNKNOWN,
    "PARDONED": StateSentenceStatus.PARDONED,
    "PENDING": StateSentenceStatus.PENDING,
    "PRESENT WITHOUT INFO": StateSentenceStatus.PRESENT_WITHOUT_INFO,
    "REVOKED": StateSentenceStatus.REVOKED,
    "SANCTIONED": StateSentenceStatus.SANCTIONED,
    "SERVING": StateSentenceStatus.SERVING,
    "SUSPENDED": StateSentenceStatus.SUSPENDED,
    "VACATED": StateSentenceStatus.VACATED,
    "INTERNAL UNKNOWN": StateSentenceStatus.INTERNAL_UNKNOWN,
}
