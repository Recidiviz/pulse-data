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


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionPeriodSupervisionType(StateEntityEnum):
    """Enum that denotes what type of supervision someone is serving at a moment in
    time."""

    ABSCONSION = state_enum_strings.state_supervision_period_supervision_type_absconsion
    BENCH_WARRANT = (
        state_enum_strings.state_supervision_period_supervision_type_bench_warrant
    )
    # TODO(#9421): The way that this information is stored may be updated when we
    #  standardize the representation of community centers
    COMMUNITY_CONFINEMENT = (
        state_enum_strings.state_supervision_period_supervision_type_community_confinement
    )
    DUAL = state_enum_strings.state_supervision_period_supervision_type_dual
    INFORMAL_PROBATION = (
        state_enum_strings.state_supervision_period_supervision_type_informal_probation
    )
    INVESTIGATION = (
        state_enum_strings.state_supervision_period_supervision_type_investigation
    )
    PAROLE = state_enum_strings.state_supervision_period_supervision_type_parole
    PROBATION = state_enum_strings.state_supervision_period_supervision_type_probation
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionPeriodSupervisionType"]:
        return _STATE_SUPERVISION_PERIOD_SUPERVISION_TYPE_MAP

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
    "a warrant for the arrest of a person on supervision. A judge can issue a bench "
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
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionPeriodAdmissionReason(StateEntityEnum):
    """Admission reasons for StateSupervisionPeriod"""

    ABSCONSION = state_enum_strings.state_supervision_period_admission_reason_absconsion
    # TODO(#12648): Change this to be RELEASE_FROM_INCARCERATION
    CONDITIONAL_RELEASE = (
        state_enum_strings.state_supervision_period_admission_reason_conditional_release
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
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionPeriodAdmissionReason"]:
        return _STATE_SUPERVISION_ADMISSION_TYPE_MAP

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
    # TODO(#12648): Update this description once this value is changed to be
    #  RELEASE_FROM_INCARCERATION
    StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE: "A period of "
    "supervision begins when a person is released through the the process of being "
    "granted parole by the parole board. The term “conditional” represents the fact "
    "that the person’s privilege of serving the rest of their sentence in the "
    "community is conditional on them following the conditions of their parole, as "
    "determined by the parole board and their parole officer. Though this should only "
    "be used on supervision periods with "
    "`StateSupervisionPeriodSupervisionType.PAROLE` "
    "or `StateSupervisionPeriodSupervisionType.DUAL`, it is currently being used for "
    "any transition from incarceration to supervision (see Issue #12648).",
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
}

# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionLevel(StateEntityEnum):
    """Possible supervision levels that a person can be supervised at."""

    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    DIVERSION = state_enum_strings.state_supervision_period_supervision_level_diversion
    # TODO(#12648): DEPRECATED - USE IN_CUSTODY INSTEAD
    INCARCERATED = (
        state_enum_strings.state_supervision_period_supervision_level_incarcerated
    )
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

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionLevel"]:
        return _STATE_SUPERVISION_LEVEL_MAP

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
    StateSupervisionLevel.INCARCERATED: "Used on a period of supervision when the "
    "person is simultaneously incarcerated (e.g. in prison for a parole board hold). "
    "TODO(#12648): THIS WILL SOON BE MERGED WITH `IN_CUSTODY`. IF YOU ARE ADDING NEW "
    "ENUM MAPPINGS, USE `IN_CUSTODY` INSTEAD.",
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
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionPeriodTerminationReason(StateEntityEnum):
    """Termination reasons for StateSupervisionPeriod"""

    ABSCONSION = (
        state_enum_strings.state_supervision_period_termination_reason_absconsion
    )
    COMMUTED = state_enum_strings.state_supervision_period_termination_reason_commuted
    DEATH = state_enum_strings.state_supervision_period_termination_reason_death
    DISCHARGE = state_enum_strings.state_supervision_period_termination_reason_discharge
    # TODO(#12648): Change this to VACATED
    DISMISSED = state_enum_strings.state_supervision_period_termination_reason_dismissed
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
    # TODO(#12648): Change this to ADMITTED_TO_INCARCERATION
    RETURN_TO_INCARCERATION = (
        state_enum_strings.state_supervision_period_termination_reason_return_to_incarceration
    )
    REVOCATION = (
        state_enum_strings.state_supervision_period_termination_reason_revocation
    )
    SUSPENSION = (
        state_enum_strings.state_supervision_period_termination_reason_suspension
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionPeriodTerminationReason"]:
        return _STATE_SUPERVISION_PERIOD_TERMINATION_REASON_MAP

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
    StateSupervisionPeriodTerminationReason.COMMUTED: "Describes a person being "
    "discharged from supervision because their sentence has been commuted. "
    "“Commutation” is a reduction of a sentence to a lesser period of time. This is "
    "different than `PARDONED` because the conviction has not been cleared from the "
    "person’s record.",
    StateSupervisionPeriodTerminationReason.DEATH: "Used when a person is no longer on "
    "supervision because they have died.",
    StateSupervisionPeriodTerminationReason.DISCHARGE: "Describes a person being "
    "released from their supervision earlier than their sentence’s end date.",
    # TODO(#12648): Rename 'DISMISSED' to 'VACATED'
    StateSupervisionPeriodTerminationReason.DISMISSED: "Used when a person is "
    "discharged because the legal judgment on their conviction has become legally "
    "void, their conviction has been overturned, or their case has been dismissed. "
    "When a sentence is vacated, there is immediate release from any active form of "
    "incarceration or supervision related to the vacated conviction. This is distinct "
    "from `PARDONED`, because the sentence was cleared as a result of it being deemed "
    "legally void.",
    StateSupervisionPeriodTerminationReason.EXPIRATION: "Describes a person’s "
    "supervision ending because they have reached the maximum completion date on "
    "their sentence.",
    StateSupervisionPeriodTerminationReason.INVESTIGATION: "Used for all periods with "
    "a `supervision_type` of `StateSupervisionPeriodSupervisionType.INVESTIGATION`.",
    # TODO(#12648): Change this to replace 'DISMISSED' with 'VACATED'
    StateSupervisionPeriodTerminationReason.PARDONED: "Describes a person being "
    "discharged from supervision because they have been pardoned. When a person is "
    "pardoned, there is immediate release from any active form of incarceration or "
    "supervision related to the pardoned conviction. This is different from `COMMUTED` "
    "because the person’s conviction is completely cleared when they are pardoned. "
    "This is distinct from `DISMISSED`, because the conviction is still legally valid, "
    "it has just been forgiven.",
    StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION: "Used when a "
    "person’s period of absconsion has ended. See "
    "`StateSupervisionPeriodSupervisionType.ABSCONSION`.",
    StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION: "Used when a "
    "person’s supervision has ended because the person has been admitted to "
    "incarceration. If the raw data indicates that this admission is due to a "
    "revocation of supervision, then `REVOCATION` should be used. All other "
    "terminations due to admissions to incarceration from supervision should use "
    "this value.",
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
}


_STATE_SUPERVISION_PERIOD_SUPERVISION_TYPE_MAP = {
    "ABSCONSION": StateSupervisionPeriodSupervisionType.ABSCONSION,
    "BENCH WARRANT": StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
    "COMMUNITY CONFINEMENT": StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
    "DUAL": StateSupervisionPeriodSupervisionType.DUAL,
    "EXTERNAL UNKNOWN": StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
    "INFORMAL PROBATION": StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
    "INTERNAL UNKNOWN": StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
    "INVESTIGATION": StateSupervisionPeriodSupervisionType.INVESTIGATION,
    "PAROLE": StateSupervisionPeriodSupervisionType.PAROLE,
    "PROBATION": StateSupervisionPeriodSupervisionType.PROBATION,
}


_STATE_SUPERVISION_ADMISSION_TYPE_MAP = {
    "ABSCONDED": StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    "ABSCONSION": StateSupervisionPeriodAdmissionReason.ABSCONSION,
    "CONDITIONAL RELEASE": StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
    "COURT SENTENCE": StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    "SENTENCE": StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    "EXTERNAL UNKNOWN": StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
    "INVESTIGATION": StateSupervisionPeriodAdmissionReason.INVESTIGATION,
    "TRANSFER WITHIN STATE": StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
    "TRANSFER FROM OTHER JURISDICTION": StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
    "RETURN FROM ABSCOND": StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    "RETURN FROM ABSCONSION": StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    "RETURN FROM SUSPENSION": StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
    "SUSPENDED": StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
}

_STATE_SUPERVISION_LEVEL_MAP: Dict[str, StateSupervisionLevel] = {
    "EXTERNAL UNKNOWN": StateSupervisionLevel.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionLevel.INTERNAL_UNKNOWN,
    "PRESENT WITHOUT INFO": StateSupervisionLevel.PRESENT_WITHOUT_INFO,
    "INCARCERATED": StateSupervisionLevel.INCARCERATED,
    "IN CUSTODY": StateSupervisionLevel.IN_CUSTODY,
    "MINIMUM": StateSupervisionLevel.MINIMUM,
    "MIN": StateSupervisionLevel.MINIMUM,
    "MEDIUM": StateSupervisionLevel.MEDIUM,
    "MED": StateSupervisionLevel.MEDIUM,
    "HIGH": StateSupervisionLevel.HIGH,
    "MAXIMUM": StateSupervisionLevel.MAXIMUM,
    "MAX": StateSupervisionLevel.MAXIMUM,
    "DIVERSION": StateSupervisionLevel.DIVERSION,
    "INTERSTATE COMPACT": StateSupervisionLevel.INTERSTATE_COMPACT,
    "INTERSTATE": StateSupervisionLevel.INTERSTATE_COMPACT,
    "UNSUPERVISED": StateSupervisionLevel.UNSUPERVISED,
    "LIMITED": StateSupervisionLevel.LIMITED,
    "ELECTRONIC MONITORING ONLY": StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
    "UNASSIGNED": StateSupervisionLevel.UNASSIGNED,
}

_STATE_SUPERVISION_PERIOD_TERMINATION_REASON_MAP = {
    "ABSCOND": StateSupervisionPeriodTerminationReason.ABSCONSION,
    "ABSCONDED": StateSupervisionPeriodTerminationReason.ABSCONSION,
    "ABSCONSION": StateSupervisionPeriodTerminationReason.ABSCONSION,
    "COMMUTED": StateSupervisionPeriodTerminationReason.COMMUTED,
    "DEATH": StateSupervisionPeriodTerminationReason.DEATH,
    "DECEASED": StateSupervisionPeriodTerminationReason.DEATH,
    "DISCHARGE": StateSupervisionPeriodTerminationReason.DISCHARGE,
    "DISCHARGED": StateSupervisionPeriodTerminationReason.DISCHARGE,
    "DISMISSED": StateSupervisionPeriodTerminationReason.DISMISSED,
    "EXPIRATION": StateSupervisionPeriodTerminationReason.EXPIRATION,
    "EXTERNAL UNKNOWN": StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN,
    "EXPIRED": StateSupervisionPeriodTerminationReason.EXPIRATION,
    "INTERNAL UNKNOWN": StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
    "INVESTIGATION": StateSupervisionPeriodTerminationReason.INVESTIGATION,
    "PARDONED": StateSupervisionPeriodTerminationReason.PARDONED,
    "TRANSFER WITHIN STATE": StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
    "TRANSFER TO OTHER JURISDICTION": StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION,
    "RETURN FROM ABSCONSION": StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
    "RETURN TO INCARCERATION": StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
    "REVOCATION": StateSupervisionPeriodTerminationReason.REVOCATION,
    "REVOKED": StateSupervisionPeriodTerminationReason.REVOCATION,
    "SUSPENDED": StateSupervisionPeriodTerminationReason.SUSPENSION,
    "SUSPENSION": StateSupervisionPeriodTerminationReason.SUSPENSION,
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
    if StateSupervisionPeriodSupervisionType.BENCH_WARRANT in supervision_types:
        return StateSupervisionPeriodSupervisionType.BENCH_WARRANT
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
        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
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
    ]

    if admission_reason in official_admissions:
        return True
    if admission_reason in non_official_admissions:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodAdmissionReason value: {admission_reason}"
    )
