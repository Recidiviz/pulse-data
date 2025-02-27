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


@unique
class StateSentenceStatus(StateEntityEnum):
    """State Sentence Status used in v1 of sentencing schema (State Incarceration/Supervision Sentence)."""

    AMENDED = state_enum_strings.state_sentence_status_amended
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

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a sentence."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SENTENCE_STATUS_VALUE_DESCRIPTIONS


_STATE_SENTENCE_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSentenceStatus.AMENDED: "Describes a sentence that has been amended. This usually"
    " happens when a sentence has been extended or otherwise modified, and the original "
    "sentence record is closed and a new amended sentence record is created.  It is also"
    " possible that a sentence is successively amended multiple times and multiple new "
    "amended sentence records created, so this value should be used for all of the "
    "earlier sentence records that are closed due to sentence modification.",
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


@unique
class StateSentenceType(StateEntityEnum):
    """State Sentence Types used in v2 of sentencing schema (StateSentence)."""

    # Incarceration Types
    COUNTY_JAIL = state_enum_strings.state_sentence_type_county_jail
    FEDERAL_PRISON = state_enum_strings.state_sentence_type_federal_prison
    STATE_PRISON = state_enum_strings.state_sentence_type_state_prison

    # Supervision Types
    PAROLE = state_enum_strings.state_sentence_type_parole
    PROBATION = state_enum_strings.state_sentence_type_probation
    COMMUNITY_CORRECTIONS = state_enum_strings.state_sentence_type_community_corrections

    # Non-time Types
    COMMUNITY_SERVICE = state_enum_strings.state_sentence_type_community_service
    FINES_RESTITUTION = state_enum_strings.state_sentence_type_fines_restitution

    # Other Types
    SPLIT = state_enum_strings.state_sentence_type_split
    TREATMENT = state_enum_strings.state_sentence_type_treatment
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of a state sentence for incarceration, supervision, and non-time sentencing."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SENTENCE_TYPE_VALUE_DESCRIPTIONS


_STATE_SENTENCE_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSentenceType.COUNTY_JAIL: (
        "Used when a person has been sentenced by the court to serve incarceration in a county jail."
    ),
    StateSentenceType.FEDERAL_PRISON: (
        "Used when a person has been sentenced by the court to serve incarceration in a federal prison."
    ),
    StateSentenceType.STATE_PRISON: (
        "Used when a person has been sentenced by the court to serve incarceration in a state prison."
    ),
    StateSentenceType.PAROLE: (
        "Used when a person is serving the remaining portion of an incarceration "
        "sentence in the community. The person’s release from prison is conditional on "
        "following certain supervision requirements as determined by the parole "
        "board and the person’s supervision officer. All periods of time spent on parole "
        "are legally associated with a sentence to incarceration."
    ),
    StateSentenceType.PROBATION: (
        "Used when a person has been "
        "sentenced by the court to a period of supervision - often in lieu of being "
        "sentenced to incarceration. Individuals on probation report to a supervision "
        "officer, and must follow the conditions of their supervision as determined by "
        "the judge and person’s supervision officer."
    ),
    StateSentenceType.COMMUNITY_CORRECTIONS: (
        "Used when a person has been sentenced by the court to community-based supervision and/or treatment services."
    ),
    StateSentenceType.COMMUNITY_SERVICE: (
        "Used when a person has been sentenced by the court to community-based supervision and/or treatment services."
    ),
    StateSentenceType.FINES_RESTITUTION: (
        "At sentencing, the judge enters an 'Order for Restitution,' directing the person to reimburse "
        "some or all of the offense-related financial losses. Compliance with the Order of Restitution automatically "
        "becomes a condition of the person's probation or supervised release. However, even before the person is "
        "released from prison, he or she is encouraged to begin repaying restitution by participating in the "
        "Inmate Financial Responsibility Program. Through this program, a percentage of the person's prison wages "
        "is applied to his or her restitution obligations."
    ),
    StateSentenceType.SPLIT: "Used when a person has been sentenced by the court with a split sentence.",
    StateSentenceType.TREATMENT: "Used when a person has been sentenced by the court to a treatment program.",
    StateSentenceType.INTERNAL_UNKNOWN: "Used when the type of sentence is unknown to us, Recidiviz.",
    StateSentenceType.EXTERNAL_UNKNOWN: "Used when the type of sentence is unknown to the state.",
}
