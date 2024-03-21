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

"""Constants related to a StateSupervisionPeriod."""
from enum import unique
from typing import Dict, Optional, Set

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateSupervisionPeriodSupervisionType(StateEntityEnum):
    """Enum that denotes what type of supervision someone is serving at a moment in
    time."""

    ABSCONSION = state_enum_strings.state_supervision_period_supervision_type_absconsion
    # TODO(#26027): Rename BENCH_WARRANT to something more descriptive to differentiate
    # it from WARRANT_STATUS
    BENCH_WARRANT = (
        state_enum_strings.state_supervision_period_supervision_type_bench_warrant
    )
    # TODO(#9421): The way that this information is stored may be updated when we
    #  standardize the representation of community centers
    COMMUNITY_CONFINEMENT = (
        state_enum_strings.state_supervision_period_supervision_type_community_confinement
    )
    DUAL = state_enum_strings.state_supervision_period_supervision_type_dual
    DEPORTED = state_enum_strings.state_supervision_period_supervision_type_deported
    INFORMAL_PROBATION = (
        state_enum_strings.state_supervision_period_supervision_type_informal_probation
    )
    INVESTIGATION = (
        state_enum_strings.state_supervision_period_supervision_type_investigation
    )
    PAROLE = state_enum_strings.state_supervision_period_supervision_type_parole
    PROBATION = state_enum_strings.state_supervision_period_supervision_type_probation
    WARRANT_STATUS = (
        state_enum_strings.state_supervision_period_supervision_type_warrant_status
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of supervision a person is on."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_PERIOD_SUPERVISION_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_PERIOD_SUPERVISION_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionPeriodSupervisionType.ABSCONSION: "Used when a person is "
    "absconding (when the person on supervision has stopped reporting to their "
    "supervising officer, and the officer cannot contact or locate them).",
    StateSupervisionPeriodSupervisionType.BENCH_WARRANT: "Used when a judge has issued "
    "a warrant for the arrest of a person on supervision, such that the person is not "
    "on active supervision (similar to absconsion status). A judge can issue a bench "
    "warrant when an individual on supervision violates the rules of the court, most "
    "often when they fail to show up to court. The police can treat this similar to "
    "an open arrest warrant and use it to bring an individual back in front of the "
    "judge.",
    StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT: "Describes a type of "
    "supervision where the person is being monitored while they are confined in the "
    "community instead of being incarcerated in a state prison facility. This happens "
    "when a person is transferred from prison into either a home or a facility in the "
    "community before they have been legally granted parole or released from "
    "incarceration. During this time they are monitored by supervision officers and "
    "not by correctional officers. They are still serving out the incarceration "
    "portion of their sentence, and are doing so confined in the community instead of "
    "in prison. Any information about the location of the person is captured in the "
    "overlapping incarceration period, if one exists. Some examples include the "
    "“Community Placement Program“ in US_ND and the “Supervised Community Confinement "
    "Program“ in US_ME.",
    StateSupervisionPeriodSupervisionType.DUAL: "If a person is serving both "
    "`PROBATION` and `PAROLE` at the same time, this may be modeled with a single "
    "supervision period if the person is reporting to a single supervising officer. "
    "When this happens, `DUAL` is used to describe the fact that the person is "
    "simultaneously on probation and parole.",
    StateSupervisionPeriodSupervisionType.DEPORTED: "Describes when a person is deported "
    "while on supervision.",
    StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION: "A type of supervision "
    "where the person is not formally supervised and does not have to regularly "
    "report to a supervision officer. The person does have certain conditions "
    "associated with their supervision, that when violated can lead to revocations. "
    "Might also be called “Court Probation“.",
    StateSupervisionPeriodSupervisionType.INVESTIGATION: "Describes a type of "
    "supervision where a person has not yet been sentenced. A person may be on "
    "investigative supervision between their arrest and trial, or between their "
    "conviction and sentencing hearing. During this time the person is often "
    "meeting with a supervision officer, who may be conducting risk assessments "
    "or interviewing the individual to provide more information to the judge.",
    StateSupervisionPeriodSupervisionType.PAROLE: "Describes the type of supervision "
    "where someone is serving the remaining portion of an incarceration sentence in "
    "the community. The person’s release from prison is conditional on them following "
    "certain supervision requirements as determined by the parole board and the "
    "person’s supervision officer.",
    StateSupervisionPeriodSupervisionType.PROBATION: "Describes the type of "
    "supervision where someone has been sentenced by the court to a period of "
    "supervision - often in lieu of being sentenced to incarceration. Individuals "
    "on probation report to a supervision officer, and must follow the conditions of "
    "their supervision as determined by the judge and person’s supervision officer.",
    StateSupervisionPeriodSupervisionType.WARRANT_STATUS: "Used when a person has been "
    "issued a warrant in response to some behavior, but is still on active supervision.",
}


@unique
class StateSupervisionPeriodAdmissionReason(StateEntityEnum):
    """Admission reasons for StateSupervisionPeriod"""

    ABSCONSION = state_enum_strings.state_supervision_period_admission_reason_absconsion
    RELEASE_FROM_INCARCERATION = (
        state_enum_strings.state_supervision_period_admission_reason_release_from_incarceration
    )
    COURT_SENTENCE = (
        state_enum_strings.state_supervision_period_admission_reason_court_sentence
    )
    # TODO(#3276): Remove this enum once we've completely transitioned to using
    #  StateSupervisionPeriodSupervisionType for Investigation
    INVESTIGATION = (
        state_enum_strings.state_supervision_period_admission_reason_investigation
    )
    TRANSFER_FROM_OTHER_JURISDICTION = (
        state_enum_strings.state_supervision_period_admission_reason_transfer_from_other_jurisdiction
    )
    TRANSFER_WITHIN_STATE = (
        state_enum_strings.state_supervision_period_admission_reason_transfer_within_state
    )
    RETURN_FROM_ABSCONSION = (
        state_enum_strings.state_supervision_period_admission_reason_return_from_absconsion
    )
    RETURN_FROM_SUSPENSION = (
        state_enum_strings.state_supervision_period_admission_reason_return_from_suspension
    )
    RETURN_FROM_WEEKEND_CONFINEMENT = (
        state_enum_strings.state_supervision_period_admission_reason_return_from_weekend_confinement
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the period of supervision has started."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_ADMISSION_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_ADMISSION_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionPeriodAdmissionReason.ABSCONSION: "This is used when a person on "
    "supervision has started absconding (when the person has stopped reporting to "
    "their supervising officer, and the officer cannot contact or locate them).",
    StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION: "A period of "
    "supervision begins directly following a period of incarceration. This could be"
    " a release onto parole but could also be used, for example, when someone is "
    "released from incarceration to serve a stacked probation sentence.",
    StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: "Used when a period of "
    "supervision has started because the person was sentenced to serve some "
    "form of supervision by the court.",
    StateSupervisionPeriodAdmissionReason.INVESTIGATION: "Used for all periods with a "
    "`supervision_type` of `StateSupervisionPeriodSupervisionType.INVESTIGATION`.",
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION: "Used when a "
    "person’s supervision has resumed after a period of absconsion. See "
    "`StateSupervisionPeriodSupervisionType.ABSCONSION`.",
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION: "Used when a "
    "person’s supervision has resumed after a period of time in which supervision was "
    "suspended. In some instances supervision may be suspended while a person awaits "
    "the result of a revocation hearing (from the court or parole board). During this "
    "time, the person is usually not reporting to their supervising officer, and also "
    "not earning time against their sentence.",
    StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: "Used when "
    "a period of supervision has started in the given state because the person was on "
    "supervision elsewhere (usually in another state), and has been transferred to "
    "report to a supervising officer within the given state.",
    StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE: "Used when a new "
    "period of supervision has started because the person on supervision has been "
    "transferred from one period of supervision to another, where both periods are "
    "supervised by officers within the given state. This is typically used when a "
    "person has transferred to a new supervising officer, or when an attribute of the "
    "supervision period has changed (e.g. when the person is downgraded to a lower "
    "`StateSupervisionLevel`).",
    StateSupervisionPeriodAdmissionReason.RETURN_FROM_WEEKEND_CONFINEMENT: "Used in "
    "cases where a person is released to supervision for the work week after weekend "
    "confinement.",
}


@unique
class StateSupervisionPeriodLegalAuthority(StateEntityEnum):
    """State Legal Authority for a period of supervision"""

    COUNTY = state_enum_strings.state_supervision_period_legal_authority_county
    COURT = state_enum_strings.state_supervision_period_legal_authority_court
    FEDERAL = state_enum_strings.state_supervision_period_legal_authority_federal
    OTHER_COUNTRY = (
        state_enum_strings.state_supervision_period_legal_authority_other_country
    )
    OTHER_STATE = (
        state_enum_strings.state_supervision_period_legal_authority_other_state
    )
    STATE_PRISON = (
        state_enum_strings.state_supervision_period_legal_authority_state_prison
    )
    SUPERVISION_AUTHORITY = (
        state_enum_strings.state_supervision_period_legal_authority_supervision_authority
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The type of government entity responsible for making decisions about "
            "a person’s path through the system. Not necessarily the same entity  "
            "responsible for the person during this period of time."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_LEGAL_AUTHORITY_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_LEGAL_AUTHORITY_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionPeriodLegalAuthority.COUNTY: "Describes a county-level authority, "
    "usually a county jail entity. This is typically used when a person is under the "
    "legal authority of a county, though not necessarily in their custody.",
    StateSupervisionPeriodLegalAuthority.COURT: "Describes a court level authority, "
    "should be used when the court has legal authority to make decision's about a "
    "person's path in the system but they are not necessarily in court's custody",
    StateSupervisionPeriodLegalAuthority.FEDERAL: "Describes a federal level authority, "
    "usually a federal facility. This is typical used when the legal authority comes from "
    "the federal level but a person might not necessarily be in federal custody.",
    StateSupervisionPeriodLegalAuthority.OTHER_COUNTRY: "Indicates that another country "
    "has legal authority over this person, even if they are in custody or on supervision elsewhere.",
    StateSupervisionPeriodLegalAuthority.OTHER_STATE: "Indicates that another state "
    "has legal authority over this person, even if they are in custody or on supervision "
    "in another state.",
    StateSupervisionPeriodLegalAuthority.STATE_PRISON: "Indicates that the state prison has "
    "legal authority over this person even if they are in a facility or on supervision elsewhere.",
    StateSupervisionPeriodLegalAuthority.SUPERVISION_AUTHORITY: "Indicates that a supervision authority "
    "has legal authority over this person even if they may be housed elsewhere.",
}


@unique
class StateSupervisionLevel(StateEntityEnum):
    """Possible supervision levels that a person can be supervised at."""

    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    DIVERSION = state_enum_strings.state_supervision_period_supervision_level_diversion
    # TODO(#9421): Re-evaluate the use of this value when we standardize the
    #  representation of community centers
    IN_CUSTODY = (
        state_enum_strings.state_supervision_period_supervision_level_in_custody
    )
    INTERSTATE_COMPACT = (
        state_enum_strings.state_supervision_period_supervision_level_interstate_compact
    )
    ELECTRONIC_MONITORING_ONLY = (
        state_enum_strings.state_supervision_period_supervision_level_electronic_monitoring_only
    )
    LIMITED = state_enum_strings.state_supervision_period_supervision_level_limited
    MINIMUM = state_enum_strings.state_supervision_period_supervision_level_minimum
    MEDIUM = state_enum_strings.state_supervision_period_supervision_level_medium
    HIGH = state_enum_strings.state_supervision_period_supervision_level_high
    MAXIMUM = state_enum_strings.state_supervision_period_supervision_level_maximum
    UNSUPERVISED = (
        state_enum_strings.state_supervision_period_supervision_level_unsupervised
    )
    UNASSIGNED = (
        state_enum_strings.state_supervision_period_supervision_level_unassigned
    )
    WARRANT = state_enum_strings.state_supervision_period_supervision_level_warrant
    ABSCONSION = (
        state_enum_strings.state_supervision_period_supervision_level_absconsion
    )
    INTAKE = state_enum_strings.state_supervision_period_supervision_level_intake
    RESIDENTIAL_PROGRAM = (
        state_enum_strings.state_supervision_period_supervision_level_residential_program
    )
    FURLOUGH = state_enum_strings.state_supervision_period_supervision_level_furlough

    def __lt__(self, other: "StateSupervisionLevel") -> bool:
        self_ranking = self.get_comparable_level_rankings().get(self)
        other_ranking = self.get_comparable_level_rankings().get(other)
        if self_ranking is None or other_ranking is None:
            raise NotImplementedError(
                "Cannot compare non-comparable StateSupervisionLevel"
            )
        return self_ranking < other_ranking

    def __gt__(self, other: "StateSupervisionLevel") -> bool:
        return other < self

    def __le__(self, other: "StateSupervisionLevel") -> bool:
        return self == other or self < other

    def __ge__(self, other: "StateSupervisionLevel") -> bool:
        return other <= self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StateSupervisionLevel):
            return False
        return self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def is_comparable(self) -> bool:
        return self in self.get_comparable_level_rankings()

    @staticmethod
    def get_comparable_level_rankings() -> Dict["StateSupervisionLevel", int]:
        return {
            StateSupervisionLevel.LIMITED: 0,
            StateSupervisionLevel.MINIMUM: 1,
            StateSupervisionLevel.MEDIUM: 2,
            StateSupervisionLevel.HIGH: 3,
            StateSupervisionLevel.MAXIMUM: 4,
        }

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The severity with which the person is being supervised, "
            "or the category of the supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_LEVEL_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_LEVEL_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionLevel.DIVERSION: "Used when a person is being supervised as a "
    "part of a specific diversion program. Diversion programs provide an opportunity "
    "for an individual to avoid being convicted of an offense by completing "
    "certain programmatic requirements (like drug treatment or mental health "
    "treatment, for example). An individual typically has their charges dismissed and "
    "record expunged upon successful completion of a diversion program.",
    StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY: "Describes a level of "
    "supervision where the person is being electronically monitored (via an "
    "ankle monitor, for example), but otherwise has no regular contact with a "
    "supervising officer.",
    StateSupervisionLevel.HIGH: "Used when a person is on a high level of "
    "supervision that is not the maximum level of supervision, as defined by the "
    "state.",
    StateSupervisionLevel.INTERSTATE_COMPACT: "Used on a period of supervision when "
    "the person is being supervised in a state that is different than the state where "
    "they were sentenced. For example, this is used in Idaho when someone is being "
    "supervised by an officer in Idaho on behalf of the state of Texas, "
    "for a conviction that occurred for the person in Texas. This is *also* used in "
    "Idaho data in cases where a person is being supervised by an officer in Texas on "
    "behalf of the state of Idaho. The `StateCustodialAuthority` value on the period "
    "determines whether the person is being supervised in the state or in another "
    "state.",
    StateSupervisionLevel.IN_CUSTODY: "Used on a period of supervision when the "
    "person is simultaneously in some form of custody (e.g. in a county jail, in a "
    "state prison for a parole board hold, in an ICE detainer, etc.).",
    StateSupervisionLevel.LIMITED: "Used when a person is on the lowest level of "
    "supervision, as defined by the state. Typically includes very minimal (e.g. "
    "once a year) contact with a supervising officer. May also be called "
    "“administrative supervision” in some states.",
    StateSupervisionLevel.MAXIMUM: "Used when a person is on the maximum level "
    "of supervision, as defined by the state.",
    StateSupervisionLevel.MEDIUM: "Used when a person is on a medium level of "
    "supervision, as defined by the state.",
    StateSupervisionLevel.MINIMUM: "Used when a person is on a low level of "
    "supervision, as defined by the state.",
    StateSupervisionLevel.UNASSIGNED: "Unassigned is used when the data explicitly "
    "indicates that the supervision level is unassigned, as opposed to when there is "
    "not enough data to determine what the supervision level is. If a value in the "
    "state's data doesn't map to one of our `StateSupervisionLevel` options, "
    "then `INTERNAL_UNKNOWN` should be used.",
    StateSupervisionLevel.UNSUPERVISED: "Used when a person is on "
    "supervision but is not being actively supervised by a supervision officer. "
    "This is distinct from `UNASSIGNED` because the person has been “assigned” to be "
    "explicitly unsupervised. A person may be unsupervised if their supervision has "
    "been suspended.",
    StateSupervisionLevel.WARRANT: "The corresponding supervision level to be mapped to"
    "`StateSupervisionPeriodSupervisionType.BENCH_WARRANT`.",
    StateSupervisionLevel.ABSCONSION: "The corresponding supervision level to be mapped to"
    "`StateSupervisionPeriodSupervisionType.ABSCONSION`.",
    StateSupervisionLevel.INTAKE: "These are usually people within the first 45 days of "
    "supervision, they have a parole officer assigned. In some states, they are assigned"
    " a specialized intake officer, in other’s it’s just a regular parole officer. There"
    " may be special conditions during this period, is distinct from unassigned because "
    "they have a parole officer.",
    StateSupervisionLevel.RESIDENTIAL_PROGRAM: "This person is participating in some "
    "intensive program (treatment or other) where the person is spending a lot of time "
    "in one location so the contact standards for their PO may be reduced. This may "
    "include a halfway house where they person may leave but returns to the halfway house"
    " to sleep every night. This would not include Day Reporting programs where someone "
    "spends many hours a day at a Day Reporting Center but returns home at night. This "
    "also would not include any in-facility treatment programs (i.e. in a county jail or"
    " state prison).",
    StateSupervisionLevel.FURLOUGH: "Special circumstance early or temporary release from"
    " prison/ incarceration. There may be special conditions (i.e. family, medical, "
    "behavior, etc.) that enable a person to be eligible for this, and there’s typically"
    " stricter terms of supervision.",
}


@unique
class StateSupervisionPeriodTerminationReason(StateEntityEnum):
    """Termination reasons for StateSupervisionPeriod"""

    ABSCONSION = (
        state_enum_strings.state_supervision_period_termination_reason_absconsion
    )
    ADMITTED_TO_INCARCERATION = (
        state_enum_strings.state_supervision_period_termination_reason_admitted_to_incarceration
    )
    COMMUTED = state_enum_strings.state_supervision_period_termination_reason_commuted
    DEATH = state_enum_strings.state_supervision_period_termination_reason_death
    DISCHARGE = state_enum_strings.state_supervision_period_termination_reason_discharge
    EXPIRATION = (
        state_enum_strings.state_supervision_period_termination_reason_expiration
    )

    # TODO(#3276): Remove this enum once we've completely transitioned to using
    #  StateSupervisionPeriodSupervisionType for Investigation
    INVESTIGATION = (
        state_enum_strings.state_supervision_period_termination_reason_investigation
    )
    PARDONED = state_enum_strings.state_supervision_period_termination_reason_pardoned
    TRANSFER_TO_OTHER_JURISDICTION = (
        state_enum_strings.state_supervision_period_termination_reason_transfer_to_other_jurisdiction
    )
    TRANSFER_WITHIN_STATE = (
        state_enum_strings.state_supervision_period_termination_reason_transfer_within_state
    )
    RETURN_FROM_ABSCONSION = (
        state_enum_strings.state_supervision_period_termination_reason_return_from_absconsion
    )
    REVOCATION = (
        state_enum_strings.state_supervision_period_termination_reason_revocation
    )
    SUSPENSION = (
        state_enum_strings.state_supervision_period_termination_reason_suspension
    )
    VACATED = state_enum_strings.state_supervision_period_termination_reason_vacated
    WEEKEND_CONFINEMENT = (
        state_enum_strings.state_supervision_period_termination_reason_weekend_confinement
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the period of supervision has ended."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_TERMINATION_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_TERMINATION_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionPeriodTerminationReason.ABSCONSION: "Used when a person is "
    "absconding (when the person on supervision has stopped reporting to their "
    "supervising officer, and the officer cannot contact or locate them).",
    StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION: "Used when a "
    "person’s supervision has ended because the person has been admitted to "
    "incarceration. If the raw data indicates that this admission is due to a "
    "revocation of supervision, then `REVOCATION` should be used. All other "
    "terminations due to admissions to incarceration from supervision should use "
    "this value.",
    StateSupervisionPeriodTerminationReason.COMMUTED: "Describes a person being "
    "discharged from supervision because their sentence has been commuted. "
    "“Commutation” is a reduction of a sentence to a lesser period of time. This is "
    "different than `PARDONED` because the conviction has not been cleared from the "
    "person’s record.",
    StateSupervisionPeriodTerminationReason.DEATH: "Used when a person is no longer on "
    "supervision because they have died.",
    StateSupervisionPeriodTerminationReason.DISCHARGE: "Describes a person being "
    "released from their supervision earlier than their sentence’s end date.",
    StateSupervisionPeriodTerminationReason.EXPIRATION: "Describes a person’s "
    "supervision ending because they have reached the maximum completion date on "
    "their sentence.",
    StateSupervisionPeriodTerminationReason.INVESTIGATION: "Used for all periods with "
    "a `supervision_type` of `StateSupervisionPeriodSupervisionType.INVESTIGATION`.",
    StateSupervisionPeriodTerminationReason.PARDONED: "Describes a person being "
    "discharged from supervision because they have been pardoned. When a person is "
    "pardoned, there is immediate release from any active form of incarceration or "
    "supervision related to the pardoned conviction. This is different from `COMMUTED` "
    "because the person’s conviction is completely cleared when they are pardoned. "
    "This is distinct from `VACATED`, because the conviction is still legally valid, "
    "it has just been forgiven.",
    StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION: "Used when a "
    "person’s period of absconsion has ended. See "
    "`StateSupervisionPeriodSupervisionType.ABSCONSION`.",
    StateSupervisionPeriodTerminationReason.REVOCATION: "Describes a person’s "
    "supervision ending because it has been revoked by either the court or the "
    "parole board.",
    StateSupervisionPeriodTerminationReason.SUSPENSION: "Used when a person’s "
    "supervision has been suspended. In some instances supervision may be suspended "
    "while a person awaits the result of a revocation hearing (from the court or "
    "parole board). During this time, the person is usually not reporting to their "
    "supervising officer, and also not earning time against their sentence.",
    StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION: "Used when "
    "a period of supervision has ended in the given state because the person has been "
    "transferred to report to a supervising officer elsewhere (usually in "
    "another state).",
    StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE: "Used when a "
    "period of supervision has ended because the person on supervision has been "
    "transferred from one period of supervision to another, where both periods are "
    "supervised by officers within the given state. This is typically used when a "
    "person has transferred to a new supervising officer, or when an attribute of the "
    "supervision period has changed (e.g. when the person is downgraded to a lower "
    "`StateSupervisionLevel`).",
    StateSupervisionPeriodTerminationReason.VACATED: "Used when a person is "
    "discharged because the legal judgment on their conviction has become legally "
    "void, their conviction has been overturned, or their case has been dismissed. "
    "When a sentence is vacated, there is immediate release from any active form of "
    "incarceration or supervision related to the vacated conviction. This is distinct "
    "from `PARDONED`, because the sentence was cleared as a result of it being deemed "
    "legally void.",
    StateSupervisionPeriodTerminationReason.WEEKEND_CONFINEMENT: "End of supervision "
    "period to return to confinement for the weekend, used for individuals who are out "
    "of prison during the week for work and return on weekends.",
}


def get_most_relevant_supervision_type(
    supervision_types: Set[StateSupervisionPeriodSupervisionType],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Return the prioritized supervision type from a list of types"""
    if not supervision_types:
        return None

    if StateSupervisionPeriodSupervisionType.DUAL in supervision_types:
        return StateSupervisionPeriodSupervisionType.DUAL
    if (
        StateSupervisionPeriodSupervisionType.PROBATION in supervision_types
        and StateSupervisionPeriodSupervisionType.PAROLE in supervision_types
    ):
        return StateSupervisionPeriodSupervisionType.DUAL

    if StateSupervisionPeriodSupervisionType.PAROLE in supervision_types:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if StateSupervisionPeriodSupervisionType.PROBATION in supervision_types:
        return StateSupervisionPeriodSupervisionType.PROBATION
    if StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT in supervision_types:
        return StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT
    if StateSupervisionPeriodSupervisionType.INVESTIGATION in supervision_types:
        return StateSupervisionPeriodSupervisionType.INVESTIGATION
    if StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION in supervision_types:
        return StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION
    if StateSupervisionPeriodSupervisionType.WARRANT_STATUS in supervision_types:
        return StateSupervisionPeriodSupervisionType.WARRANT_STATUS
    if StateSupervisionPeriodSupervisionType.BENCH_WARRANT in supervision_types:
        return StateSupervisionPeriodSupervisionType.BENCH_WARRANT
    if StateSupervisionPeriodSupervisionType.DEPORTED in supervision_types:
        return StateSupervisionPeriodSupervisionType.DEPORTED
    if StateSupervisionPeriodSupervisionType.ABSCONSION in supervision_types:
        return StateSupervisionPeriodSupervisionType.ABSCONSION
    if StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN in supervision_types:
        return StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN
    if StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN in supervision_types:
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    raise ValueError(
        f"Unexpected Supervision type in provided supervision_types set: [{supervision_types}]"
    )


def is_official_supervision_admission(
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason],
) -> bool:
    """Returns whether or not the |admission_reason| is considered an official start of supervision."""
    if not admission_reason:
        return False

    # A supervision period that has one of these admission reasons indicates the official start of supervision
    official_admissions = [
        StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
    ]

    non_official_admissions = [
        StateSupervisionPeriodAdmissionReason.ABSCONSION,
        StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
        StateSupervisionPeriodAdmissionReason.INVESTIGATION,
        StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
        StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_WEEKEND_CONFINEMENT,
    ]

    if admission_reason in official_admissions:
        return True
    if admission_reason in non_official_admissions:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodAdmissionReason value: {admission_reason}"
    )
