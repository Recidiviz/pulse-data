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
    """The given status of a single state sentence for a single period of time."""

    AMENDED = state_enum_strings.state_sentence_status_amended
    COMMUTED = state_enum_strings.state_sentence_status_commuted
    COMPLETED = state_enum_strings.state_sentence_status_completed
    PARDONED = state_enum_strings.state_sentence_status_pardoned
    PENDING = state_enum_strings.state_sentence_status_pending
    DEATH = state_enum_strings.state_sentence_status_death
    EXECUTION = state_enum_strings.state_sentence_status_execution
    # Only use REVOKED when a person is re-sentenced because of revocation,
    # otherwise use SERVING.
    REVOKED = state_enum_strings.state_sentence_status_revoked
    SANCTIONED = state_enum_strings.state_sentence_status_sanctioned
    SERVING = state_enum_strings.state_sentence_status_serving
    SUSPENDED = state_enum_strings.state_sentence_status_suspended
    VACATED = state_enum_strings.state_sentence_status_vacated
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    IMPOSED_PENDING_SERVING = (
        state_enum_strings.state_sentence_status_imposed_pending_serving
    )
    NON_CREDIT_SERVING = state_enum_strings.state_sentence_status_non_credit_serving
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a sentence."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SENTENCE_STATUS_VALUE_DESCRIPTIONS

    @property
    def is_terminating_status(self) -> bool:
        return _STATE_SENTENCE_STATUS_VALUE_TERMINATES[self]

    @property
    def is_considered_serving_status(self) -> bool:
        return _STATE_SENTENCE_STATUS_VALUE_DESIGNATES_SERVING[self]


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
    StateSentenceStatus.IMPOSED_PENDING_SERVING: "Describes a sentence that has been imposed "
    "consecutively to another sentence, but has not begun serving. This means the parent sentence "
    "has an initial serving status and no terminating status.",
    # This Enum value was created for TN where they created a board to retroactively remove
    # credits from people's sentences.
    StateSentenceStatus.NON_CREDIT_SERVING: "Describes a point in time for a sentence that "
    "shows a person was serving their sentence, but the state has decided they do not receive "
    "credit towards serving that sentence. This can be due to not meeting the requirements towards "
    "credit or the state revoking credit after a person has actually been serving their sentence.",
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
    StateSentenceStatus.DEATH: "Describes a sentence that has ended because the person"
    "serving has died.",
    StateSentenceStatus.EXECUTION: "Describes a sentence has ended because the person "
    "serving has been executed.",
}

# We enumerate every enum option here to ensure all are accounted for and tested.
_STATE_SENTENCE_STATUS_VALUE_TERMINATES: Dict[StateEntityEnum, bool] = {
    StateSentenceStatus.COMPLETED: True,
    StateSentenceStatus.PARDONED: True,
    StateSentenceStatus.REVOKED: True,
    StateSentenceStatus.VACATED: True,
    StateSentenceStatus.DEATH: True,
    StateSentenceStatus.EXECUTION: True,
    # These are not terminating statuses
    StateSentenceStatus.NON_CREDIT_SERVING: False,
    StateSentenceStatus.AMENDED: False,
    StateSentenceStatus.COMMUTED: False,
    StateSentenceStatus.PENDING: False,
    StateSentenceStatus.PRESENT_WITHOUT_INFO: False,
    StateSentenceStatus.SANCTIONED: False,
    StateSentenceStatus.SERVING: False,
    StateSentenceStatus.SUSPENDED: False,
    StateSentenceStatus.IMPOSED_PENDING_SERVING: False,
    StateSentenceStatus.EXTERNAL_UNKNOWN: False,
    StateSentenceStatus.INTERNAL_UNKNOWN: False,
}

# We enumerate every enum option here to ensure all are accounted for and tested.
_STATE_SENTENCE_STATUS_VALUE_DESIGNATES_SERVING: Dict[StateEntityEnum, bool] = {
    StateSentenceStatus.AMENDED: True,
    StateSentenceStatus.COMMUTED: True,
    StateSentenceStatus.SANCTIONED: True,
    StateSentenceStatus.SERVING: True,
    # We do not consider a sentence as being served while it has one
    # of these statuses
    StateSentenceStatus.NON_CREDIT_SERVING: False,
    StateSentenceStatus.IMPOSED_PENDING_SERVING: False,
    StateSentenceStatus.COMPLETED: False,
    StateSentenceStatus.PARDONED: False,
    StateSentenceStatus.DEATH: False,
    StateSentenceStatus.EXECUTION: False,
    StateSentenceStatus.REVOKED: False,
    StateSentenceStatus.VACATED: False,
    StateSentenceStatus.SUSPENDED: False,
    StateSentenceStatus.EXTERNAL_UNKNOWN: False,
    StateSentenceStatus.INTERNAL_UNKNOWN: False,
    StateSentenceStatus.PRESENT_WITHOUT_INFO: False,
    StateSentenceStatus.PENDING: False,
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
        "Used when a person has been sentenced by the court to perform some amount of community service."
    ),
    StateSentenceType.FINES_RESTITUTION: (
        "At sentencing, the judge enters an 'Order for Restitution,' directing the person to reimburse "
        "some or all of the offense-related financial losses. Compliance with the Order of Restitution automatically "
        "becomes a condition of the person's probation or supervised release. However, even before the person is "
        "released from prison, he or she is encouraged to begin repaying restitution by participating in the "
        "Inmate Financial Responsibility Program. Through this program, a percentage of the person's prison wages "
        "is applied to his or her restitution obligations."
    ),
    StateSentenceType.SPLIT: (
        "Used when a person has been sentenced by the court with a "
        "sentence that will be served in two parts, one incarceration and the other "
        "supervision. This should only be used when the state represents  this sentence "
        "structure with a single sentence external id. In many states, this  type of sentence "
        "can be represented as an INCARCERATION sentence followed by a consecutive "
        "PROBATION/PAROLE sentence."
    ),
    StateSentenceType.TREATMENT: "Used when a person has been sentenced by the court to a treatment program.",
    StateSentenceType.INTERNAL_UNKNOWN: "Used when the type of sentence is unknown to us, Recidiviz.",
    StateSentenceType.EXTERNAL_UNKNOWN: "Used when the type of sentence is unknown to the state.",
}


@unique
class StateSentencingAuthority(StateEntityEnum):
    """
    This Enum indicates what level/locale imposed its parent sentence.
    We allow sentence to be imposed by a county court, state court,
    county or state court of a different state, or a federal court.

    Note that the sentencing authority does not need to be the same
    institution responsible for other aspects of a sentence, just the imposition.


    Example: A value of COUNTY means a county court imposed this sentence.
    This enum is only optional for parsing. We expect it to exist once entities
    are merged up.
    """

    COUNTY = state_enum_strings.state_sentencing_authority_county
    STATE = state_enum_strings.state_sentencing_authority_state
    OTHER_STATE = state_enum_strings.state_sentencing_authority_other_state
    FEDERAL = state_enum_strings.state_sentencing_authority_federal
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @property
    def is_out_of_state(self) -> bool:
        return self in (self.OTHER_STATE, self.FEDERAL)

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The authority imposing a sentence. "
            "For example, a state or federal court."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SENTENCING_AUTHORITY_VALUE_DESCRIPTIONS


_STATE_SENTENCING_AUTHORITY_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSentencingAuthority.COUNTY: "A county court within the state.",
    StateSentencingAuthority.STATE: "A state court within the state.",
    StateSentencingAuthority.OTHER_STATE: "A state or county court of a different state.",
    StateSentencingAuthority.FEDERAL: "A federal court.",
}
