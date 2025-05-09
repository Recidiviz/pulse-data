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

"""Constants related to a StateIncarcerationPeriod."""
from enum import unique
from typing import Dict, Optional

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateIncarcerationPeriodAdmissionReason(StateEntityEnum):
    """Reasons for admission to a period of incarceration."""

    ADMITTED_IN_ERROR = (
        state_enum_strings.state_incarceration_period_admission_reason_admitted_in_error
    )
    # This is an ingest-only enum, and should only be used as a placeholder if we are unable
    # to determine whether this is a sanction, revocation, or temporary custody admission.
    ADMITTED_FROM_SUPERVISION = (
        state_enum_strings.state_incarceration_period_admission_reason_admitted_from_supervision
    )
    ESCAPE = state_enum_strings.state_incarceration_period_admission_reason_escape
    NEW_ADMISSION = (
        state_enum_strings.state_incarceration_period_admission_reason_new_admission
    )
    REVOCATION = (
        state_enum_strings.state_incarceration_period_admission_reason_revocation
    )
    RETURN_FROM_ERRONEOUS_RELEASE = (
        state_enum_strings.state_incarceration_period_admission_reason_return_from_erroneous_release
    )
    # This admission type corresponds to returns from any temporary release (example: work release, furlough, etc).
    RETURN_FROM_TEMPORARY_RELEASE = (
        state_enum_strings.state_incarceration_period_admission_reason_return_from_temporary_release
    )
    RETURN_FROM_ESCAPE = (
        state_enum_strings.state_incarceration_period_admission_reason_return_from_escape
    )
    SANCTION_ADMISSION = (
        state_enum_strings.state_incarceration_period_admission_reason_sanction_admission
    )
    STATUS_CHANGE = (
        state_enum_strings.state_incarceration_period_admission_reason_status_change
    )
    TEMPORARY_CUSTODY = (
        state_enum_strings.state_incarceration_period_admission_reason_temporary_custody
    )
    TEMPORARY_RELEASE = (
        state_enum_strings.state_incarceration_period_admission_reason_temporary_release
    )
    TRANSFER = state_enum_strings.state_incarceration_period_admission_reason_transfer
    TRANSFER_FROM_OTHER_JURISDICTION = (
        state_enum_strings.state_incarceration_period_admission_reason_transfer_from_other_jurisdiction
    )
    WEEKEND_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_admission_reason_weekend_confinement
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the person is being admitted to a facility."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_PERIOD_ADMISSION_REASON_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_PERIOD_ADMISSION_REASON_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION: "This is an "
    "ingest-only enum, and should only be used as a placeholder at ingest time if we "
    "are unable to determine whether an admission from supervision to prison is a "
    "sanction, revocation, or temporary custody admission. All periods with this "
    "value must be updated by the state’s IP normalization process to set the "
    "accurate admission reason.",
    StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR: "Used when a person "
    "has been admitted into a facility erroneously.",
    StateIncarcerationPeriodAdmissionReason.ESCAPE: "Used when a person has escaped from"
    " a facility but should still be counted as incarcerated.",
    StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: "Describes admissions into "
    "prison to serve a new sentence because of a new commitment from the court.",
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE: "Used when "
    "a person is admitted after having been released, where the person was "
    "unintentionally released.",
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: "Used when a person "
    "returns to a facility after having escaped.",
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE: "Describes "
    "returns to a facility from any kind of temporary release (e.g. work release, "
    "furlough, etc.), where there is an understanding of when the person will be "
    "returning to the facility.",
    StateIncarcerationPeriodAdmissionReason.REVOCATION: "Used when a person is "
    "admitted to prison because their supervision was revoked by the court or by the "
    "parole board.",
    StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION: "A non-revocation "
    "admission from supervision to prison as a sanction response to a person "
    "violating conditions of their supervision. When used with the "
    "`StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION` value, describes "
    "being mandated by either the court or the parole board to spend a distinct, "
    "“short” period of time in prison (known as “shock incarceration”). When used "
    "with the `StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON` value, "
    "describes being mandated by either the court or the parole board to complete a "
    "treatment program in prison.<br><br>Some examples include being mandated by the "
    "parole board to complete in-facility drug treatment after failing to complete "
    "treatment in the community, or being mandated by the court to spend exactly "
    "120 days in prison as a penalty for violating conditions of one’s supervision.",
    StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE: "Used when something about "
    "a person’s incarceration has changed from a classification-standpoint. For "
    "example, this is used when the `specialized_purpose_for_incarceration` value "
    "on an IP changes, denoting that the “reason” the person is in prison has changed.",
    StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY: "Describes “temporary” "
    "admissions to a facility, where the release date is determined by something "
    "other than the person’s sentence end date. Used on all admissions to parole "
    "board holds (with `StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD`), "
    "which is a temporary situation in which a person on parole is incarcerated while "
    "awaiting a decision from the parole board as to whether they will have to remain "
    "in prison (for treatment, shock incarceration, or to serve out the rest of "
    "their sentence). Used also when a person is brought into a county jail "
    "temporarily after a probation revocation while the state determines the prison "
    "into which they will be admitted.",
    StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE: "Describes temporary release"
    " from the facility to a medical institution, county jail awaiting court hearing or "
    "another circumstance, but the person should still be considered as incarcerated during "
    " this release.",
    StateIncarcerationPeriodAdmissionReason.TRANSFER: "Used when a person is "
    "transferred between two facilities within the state.",
    StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: "Used "
    "when a person is transferred from a different jurisdiction. Used when a person "
    "is transferred from another state, or from federal custody.",
    StateIncarcerationPeriodAdmissionReason.WEEKEND_CONFINEMENT: "Start of a weekend "
    "confinement period for individuals who are out of prison during the week for work "
    "and return on weekends.",
}


@unique
class StateIncarcerationPeriodReleaseReason(StateEntityEnum):
    """Reasons for release from a period of incarceration."""

    COMMUTED = state_enum_strings.state_incarceration_period_release_reason_commuted
    COMPASSIONATE = (
        state_enum_strings.state_incarceration_period_release_reason_compassionate
    )
    # This release type corresponds to a release through the process of being granted
    # parole by the parole board
    CONDITIONAL_RELEASE = (
        state_enum_strings.state_incarceration_period_release_reason_conditional_release
    )
    COURT_ORDER = (
        state_enum_strings.state_incarceration_period_release_reason_court_order
    )
    DEATH = state_enum_strings.state_incarceration_period_release_reason_death
    ESCAPE = state_enum_strings.state_incarceration_period_release_reason_escape
    EXECUTION = state_enum_strings.state_incarceration_period_release_reason_execution
    PARDONED = state_enum_strings.state_incarceration_period_release_reason_pardoned
    RELEASED_FROM_ERRONEOUS_ADMISSION = (
        state_enum_strings.state_incarceration_period_release_reason_released_from_erroneous_admission
    )
    RELEASED_FROM_TEMPORARY_CUSTODY = (
        state_enum_strings.state_incarceration_period_release_reason_released_from_temporary_custody
    )
    RELEASED_IN_ERROR = (
        state_enum_strings.state_incarceration_period_release_reason_released_in_error
    )
    # This release type corresponds to any release onto some sort of supervision that
    # doesn't qualify as a CONDITIONAL_RELEASE (also used if we cannot determine what
    # type of release to supervision a release is)
    RELEASED_TO_SUPERVISION = (
        state_enum_strings.state_incarceration_period_release_reason_released_to_supervision
    )
    RETURN_FROM_ESCAPE = (
        state_enum_strings.state_incarceration_period_release_reason_return_from_escape
    )
    RETURN_FROM_TEMPORARY_RELEASE = (
        state_enum_strings.state_incarceration_period_release_reason_return_from_temporary_release
    )
    SENTENCE_SERVED = (
        state_enum_strings.state_incarceration_period_release_reason_sentence_served
    )
    STATUS_CHANGE = (
        state_enum_strings.state_incarceration_period_release_reason_status_change
    )
    # This release type corresponds to any temporary release (example: work release, furlough, etc).
    TEMPORARY_RELEASE = (
        state_enum_strings.state_incarceration_period_release_reason_temporary_release
    )
    TRANSFER = state_enum_strings.state_incarceration_period_release_reason_transfer
    TRANSFER_TO_OTHER_JURISDICTION = (
        state_enum_strings.state_incarceration_period_release_reason_transfer_to_other_jurisdiction
    )
    VACATED = state_enum_strings.state_incarceration_period_release_reason_vacated
    RELEASE_FROM_WEEKEND_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_release_reason_release_from_weekend_confinement
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the person is being released from a facility."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_PERIOD_RELEASE_REASON_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_PERIOD_RELEASE_REASON_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationPeriodReleaseReason.COMMUTED: "Describes a person being "
    "released from a facility because their sentence has been commuted. “Commutation” "
    "is a reduction of a sentence to a lesser period of time. This is different "
    "than `PARDONED` because the conviction has not been cleared from the person’s "
    "record.",
    StateIncarcerationPeriodReleaseReason.COMPASSIONATE: "Used when a person has been "
    "granted early release from prison because of special circumstances (defined "
    "as “extraordinary and compelling reasons” by the U.S. Sentencing Commission). "
    "Compassionate release is very rarely used, but can describe cases such as an "
    "individual being granted early release because they have a terminal illness.",
    StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: "Describes a person "
    "being released through the process of being granted parole by the parole board. "
    "The term “conditional” represents the fact that the person’s privilege of "
    "serving the rest of their sentence in the community is conditional on them "
    "following the conditions of their parole, as determined by the parole board "
    "and their parole officer.",
    StateIncarcerationPeriodReleaseReason.COURT_ORDER: "Used when a person is "
    "temporarily released because a judge has requested that the person make an "
    "appearance in court. The person may be transferred to a county jail during "
    "this time.",
    StateIncarcerationPeriodReleaseReason.DEATH: "Used when a person is no longer "
    "in a facility because they have died.",
    StateIncarcerationPeriodReleaseReason.ESCAPE: "Used when a person has escaped "
    "from a facility.",
    StateIncarcerationPeriodReleaseReason.EXECUTION: "Used when a person is no longer "
    "in a facility because they have been executed by the state.",
    StateIncarcerationPeriodReleaseReason.PARDONED: "Describes a person being "
    "released from a facility because they have been pardoned. When a person is "
    "pardoned, there is immediate release from any active form of incarceration "
    "or supervision related to the pardoned conviction. This is different from "
    "`COMMUTED` because the person’s conviction is completely cleared when they are "
    "pardoned. This is distinct from `VACATED`, because the conviction is still "
    "legally valid, it has just been forgiven.",
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION: "Used "
    "when a person is released after having been admitted into a facility erroneously.",
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: "Used when "
    "a person has been released from a period of temporary custody. See "
    "`StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY`.",
    StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR: "Used when a person has "
    "been released from a facility erroneously.",
    StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION: "Describes any "
    "release onto some sort of supervision that doesn't qualify as a "
    "`CONDITIONAL_RELEASE`. This is not common, but can be used to describe instances "
    "where a person is released onto probation, for example (i.e. they are serving a "
    "stacked probation sentence after an incarceration sentence). This is also used if "
    "we cannot determine what type of release to supervision a release is.",
    StateIncarcerationPeriodReleaseReason.RELEASE_FROM_WEEKEND_CONFINEMENT: "End of a "
    "weekend confinement period for individuals who are out of prison during the week "
    "for work and return on weekends.",
    StateIncarcerationPeriodReleaseReason.RETURN_FROM_ESCAPE: "Describes a person having "
    "been returned to the facility from having previously escaped, thus ending a period "
    "of incarceration in which they were deemed escaped from the facility.",
    StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE: "Describes a person "
    "having returned to the facility from having been previously temporarily released, "
    "for example to a hospital, to county jail awaiting court hearing, etc.",
    StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: "Describes a person being "
    "released because they have served the entirety of their sentence. This should "
    "not be used if the person is being released onto any form of supervision "
    "(see `CONDITIONAL_RELEASE` and `RELEASED_TO_SUPERVISION`).",
    StateIncarcerationPeriodReleaseReason.STATUS_CHANGE: "Used when something about "
    "a person’s incarceration has changed from a classification-standpoint. For "
    "example, this is used when the `specialized_purpose_for_incarceration` value on "
    "an IP changes, denoting that the “reason” the person is in prison has changed.",
    StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE: "Describes being released "
    "from a facility for any kind of temporary reason (e.g. work release, furlough, "
    "etc.), where there is an understanding of when the person will be returning to "
    "the facility.",
    StateIncarcerationPeriodReleaseReason.TRANSFER: "Used when a person is "
    "transferred between two facilities within the state.",
    StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION: "Used when "
    "a person is transferred to a different jurisdiction. Used when a person is "
    "transferred to another state, or to federal custody.",
    StateIncarcerationPeriodReleaseReason.VACATED: "Used when a person is released "
    "because the legal judgment on their conviction has become legally void, their "
    "conviction has been overturned, or their case has been dismissed. When a "
    "sentence is vacated, there is immediate release from any active form of "
    "incarceration or supervision related to the vacated conviction. This is distinct "
    "from `PARDONED`, because the sentence was cleared as a result of it being "
    "deemed legally void.",
}


# TODO(#3275): Update enum name to `StatePurposeForIncarceration` now that there is a 'GENERAL' option
@unique
class StateSpecializedPurposeForIncarceration(StateEntityEnum):
    """Specialized purposes for a period of incarceration"""

    GENERAL = state_enum_strings.state_specialized_purpose_for_incarceration_general
    PAROLE_BOARD_HOLD = (
        state_enum_strings.state_specialized_purpose_for_incarceration_parole_board_hold
    )
    SHOCK_INCARCERATION = (
        state_enum_strings.state_specialized_purpose_for_incarceration_shock_incarceration
    )
    TREATMENT_IN_PRISON = (
        state_enum_strings.state_specialized_purpose_for_incarceration_treatment_in_prison
    )
    TEMPORARY_CUSTODY = (
        state_enum_strings.state_specialized_purpose_for_incarceration_temporary_custody
    )
    # Denotes that someone is incarcerated as part of a program/sentence where they
    # are released during the week to go to work, then readmitted every weekend.
    WEEKEND_CONFINEMENT = (
        state_enum_strings.state_specialized_purpose_for_incarceration_weekend_confinement
    )
    # Denotes that someone is "incarcerated" for safekeeping, which means they have not yet
    # been sentenced, but are being held in a facility for their own safety or the safety of others,
    # or for medical/health/space reasons.
    SAFEKEEPING = (
        state_enum_strings.state_specialized_purpose_for_incarceration_safekeeping
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the person is in a facility."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SPECIALIZED_PURPOSE_FOR_INCARCERATION_VALUE_DESCRIPTIONS


_STATE_SPECIALIZED_PURPOSE_FOR_INCARCERATION_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSpecializedPurposeForIncarceration.GENERAL: "This person is in a facility "
    "serving a sentence, where the reason the person is in a facility does not fall "
    "into any of the other `StateSpecializedPurposeForIncarceration` categories.",
    StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD: "This person is in a "
    "facility temporarily while they await a hearing by the parole board. This is a "
    "temporary situation in which a person on parole is incarcerated while awaiting "
    "a decision from the parole board as to whether they will have to remain in "
    "prison (for treatment, shock incarceration, or to serve out the rest of their "
    "sentence), or whether they will be released back onto parole.",
    StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION: "This person is in a "
    "facility because they were mandated by either the court or the parole board to "
    "spend a distinct, “short” period of time in prison. These mandates are always "
    "for explicit amounts of time (e.g. 120 days, 9 months, etc.).",
    StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY: "This person is in "
    "facility temporarily, where the release date is determined by something other "
    "than the person’s sentence end date, and where the person is *not* in a parole "
    "board hold. For example, this is used when a person is in a county jail "
    "temporarily after a probation revocation while the state determines the prison "
    "into which they will be admitted.",
    StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON: "This person is in a "
    "facility because they were mandated by either the court or the parole board to "
    "complete a treatment program in prison.",
    StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT: "This person is in a "
    "facility as part of a program or sentence where they are released during the "
    "week, then readmitted every weekend.",
    StateSpecializedPurposeForIncarceration.SAFEKEEPING: "This person is in a facility "
    "as a 'safekeeper', which means they have not yet been sentenced, but are being "
    "held in a facility for their own safety or the safety of others, or for "
    "medical/health/space reasons.",
}


@unique
class StateIncarcerationPeriodCustodyLevel(StateEntityEnum):
    """Custody levels for incarceration periods"""

    INTAKE = state_enum_strings.state_incarceration_period_custody_level_intake
    MINIMUM = state_enum_strings.state_incarceration_period_custody_level_minimum
    RESTRICTIVE_MINIMUM = (
        state_enum_strings.state_incarceration_period_custody_level_restrictive_minimum
    )
    MEDIUM = state_enum_strings.state_incarceration_period_custody_level_medium
    CLOSE = state_enum_strings.state_incarceration_period_custody_level_close
    MAXIMUM = state_enum_strings.state_incarceration_period_custody_level_maximum
    SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_custody_level_solitary_confinement
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The level of supervision and security employed for a person held in custody."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_PERIOD_CUSTODY_LEVEL_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_PERIOD_CUSTODY_LEVEL_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationPeriodCustodyLevel.INTAKE: "Describes the level of security and "
    "supervision employed when an individual is held at a processing/reception center "
    "where they're going through the intake and classification process.",
    StateIncarcerationPeriodCustodyLevel.MINIMUM: "Describes when an individual has been"
    " determined to pose low risk to the safety of others, are not an escape risk, have "
    "minimal medical or mental health needs, and therefore require minimum security and "
    "supervision measures. The individual's movements may be unrestricted within the "
    "facility and may be allowed to participate in offsite work programs.",
    StateIncarcerationPeriodCustodyLevel.RESTRICTIVE_MINIMUM: "Describes when an individual"
    " has been determined to pose low risk to the safety of others, are a low escape "
    "risk, have low to moderate medical or mental health needs, and therefore require "
    "minimum supervision within a secure facility.",
    StateIncarcerationPeriodCustodyLevel.MEDIUM: "Describes when an individual has been "
    "determined to require a moderate level of supervision based on their offense history,"
    " minor disciplinary issues, or medical or mental health needs.",
    StateIncarcerationPeriodCustodyLevel.CLOSE: "Describes when an individual has been "
    "determined to require heightened supervision based on their conduct or offense history.",
    StateIncarcerationPeriodCustodyLevel.MAXIMUM: "Describes when an individual has been"
    " determined to require an intense level of supervision within a general population setting.",
    StateIncarcerationPeriodCustodyLevel.SOLITARY_CONFINEMENT: "Describes an administrative"
    " level of solitary confinement which is mutually exclusive with other administrative"
    " custody levels. Individuals are placed into this because they have been deemed a "
    "risk to the safety of others and/or are extremely difficult to manage in a general "
    "population setting. Note, in many states solitary confinement is not treated as a "
    "custody level but only as a cell/housing type. In those states, this field should "
    "continue to have their custody level and only their housing type should be set to "
    "solitary confinement.",
}


@unique
class StateIncarcerationPeriodHousingUnitCategory(StateEntityEnum):
    """Housing unit categories for incarceration periods"""

    SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_category_solitary_confinement
    )
    GENERAL = (
        state_enum_strings.state_incarceration_period_housing_unit_category_general
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The level of supervision and security employed for a person held in custody."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_PERIOD_HOUSING_UNIT_CATEGORY_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_PERIOD_HOUSING_UNIT_CATEGORY_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationPeriodHousingUnitCategory.SOLITARY_CONFINEMENT: "This person has been permanently assigned to a solitary confinement unit for an indeterminate amount of time.",
    StateIncarcerationPeriodHousingUnitCategory.GENERAL: "This person is in a non-specialty housing unit. Incarceration periods will generally be assigned this value by default.",
}


@unique
class StateIncarcerationPeriodHousingUnitType(StateEntityEnum):
    """Housing unit types for incarceration periods"""

    TEMPORARY_SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_type_temporary_solitary_confinement
    )
    DISCIPLINARY_SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_type_disciplinary_solitary_confinement
    )
    ADMINISTRATIVE_SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_type_administrative_solitary_confinement
    )
    PROTECTIVE_CUSTODY = (
        state_enum_strings.state_incarceration_period_housing_unit_type_protective_custody
    )
    OTHER_SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_type_other_solitary_confinement
    )
    MENTAL_HEALTH_SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_type_mental_health_solitary_confinement
    )
    HOSPITAL = state_enum_strings.state_incarceration_period_housing_unit_type_hospital
    GENERAL = state_enum_strings.state_incarceration_period_housing_unit_type_general
    ## TODO(#22252): Remove this once we have deprecated PERMANENT_SOLITARY_CONFINEMENT
    PERMANENT_SOLITARY_CONFINEMENT = (
        state_enum_strings.state_incarceration_period_housing_unit_type_permanent_solitary
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The level of supervision and security employed for a person held in custody."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_PERIOD_HOUSING_UNIT_TYPE_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_PERIOD_HOUSING_UNIT_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationPeriodHousingUnitType.TEMPORARY_SOLITARY_CONFINEMENT: "This is the placement of a person in restrictive housing that can occur for a wide range of institutional needs and likely has another bed saved for them elsewhere in the facility. For example, it can be an interim status for people pending their transfer to another institution or awaiting a judicial proceeding, to facilitate a criminal investigation, or when limited bed space in an institution necessitates the use of an otherwise empty segregation cell.",
    StateIncarcerationPeriodHousingUnitType.DISCIPLINARY_SOLITARY_CONFINEMENT: "This person is placed in restrictive housing as a form of punishment.",
    StateIncarcerationPeriodHousingUnitType.ADMINISTRATIVE_SOLITARY_CONFINEMENT: "This person is placed in restrictive housing for managerial purposes, including as a response to a person who demonstrates a chronic inability to adjust to the general population, or when authorities believe an inmate's presence in the general population may cause a serious disruption to the orderly operation of the institution.",
    StateIncarcerationPeriodHousingUnitType.PROTECTIVE_CUSTODY: "This person is placed in restrictive housing to separate vulnerable people from the general population due to personal physical safety concerns.",
    StateIncarcerationPeriodHousingUnitType.OTHER_SOLITARY_CONFINEMENT: "This person is placed in a restrictive housing unit that is not temporary but does not fit into the other categories.",
    StateIncarcerationPeriodHousingUnitType.MENTAL_HEALTH_SOLITARY_CONFINEMENT: "This person is placed in restrictive housing because of their mental health status.",
    StateIncarcerationPeriodHousingUnitType.HOSPITAL: "This person is placed in a hospital unit for medical attention.",
    StateIncarcerationPeriodHousingUnitType.GENERAL: "This person is in a non-specialty housing unit. Incarceration periods will generally be assigned this value by default.",
    ## TODO(#22252): Remove this once we have deprecated PERMANENT_SOLITARY_CONFINEMENT
    StateIncarcerationPeriodHousingUnitType.PERMANENT_SOLITARY_CONFINEMENT: "This person has been permanently assigned to a solitary confinement unit for an indeterminate amount of time.",
}


def is_commitment_from_supervision(
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason],
    allow_ingest_only_enum_values: bool = False,
) -> bool:
    """Determines if the provided admission_reason represents a type of commitment from
    supervision due to a sanction or revocation.

    When dealing with incarceration periods during ingest or during IP pre-processing
    we may encounter ingest-only enum values that need to be handled by this function.
    After IP pre-processing we do not expect to see any ingest-only enum values. The
    |allow_ingest_only_enum_values| boolean should only be set to True if this
    function is being called during ingest or during IP pre-processing. All usage of
    this function in calculations with pre-processed IPs should have
    allow_ingest_only_enum_values=False.
    """
    if not admission_reason:
        return False
    commitment_admissions = [
        StateIncarcerationPeriodAdmissionReason.REVOCATION,
        StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.WEEKEND_CONFINEMENT,
    ]
    non_commitment_admissions = [
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        StateIncarcerationPeriodAdmissionReason.ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE,
        StateIncarcerationPeriodAdmissionReason.TRANSFER,
        StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
        StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
    ]
    if admission_reason in commitment_admissions:
        return True
    if admission_reason in non_commitment_admissions:
        return False
    if (
        admission_reason
        == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    ):
        if allow_ingest_only_enum_values:
            return False

        raise ValueError(
            "ADMITTED_FROM_SUPERVISION is an ingest-only enum, and we should not "
            "see this value after IP pre-processing."
        )
    raise ValueError(
        f"Unexpected StateIncarcerationPeriodAdmissionReason {admission_reason}."
    )


def is_official_admission(
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason],
    allow_ingest_only_enum_values: bool = False,
) -> bool:
    """Returns whether or not the |admission_reason| is considered an official start of
    incarceration, i.e. the root cause for being admitted to prison at all,
    not transfers or other unknown statuses resulting in facility changes.

    When dealing with incarceration periods during ingest or during IP pre-processing
    we may encounter ingest-only enum values that need to be handled by this function.
    After IP pre-processing we do not expect to see any ingest-only enum values. The
    |allow_ingest_only_enum_values| boolean should only be set to True if this
    function is being called during ingest or during IP pre-processing. All usage of
    this function in calculations with pre-processed IPs should have
    allow_ingest_only_enum_values=False.
    """
    if not admission_reason:
        return False

    # An incarceration period that has one of these admission reasons indicates the
    # official start of incarceration
    official_admission_types = [
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.REVOCATION,
        StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
    ]

    non_official_admission_types = [
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TRANSFER,
        StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
        StateIncarcerationPeriodAdmissionReason.ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE,
        StateIncarcerationPeriodAdmissionReason.WEEKEND_CONFINEMENT,
    ]

    if admission_reason in official_admission_types:
        return True
    if admission_reason in non_official_admission_types:
        return False
    if (
        admission_reason
        == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    ):
        if allow_ingest_only_enum_values:
            return True

        raise ValueError(
            "ADMITTED_FROM_SUPERVISION is an ingest-only enum, and we should not "
            "see this value after IP pre-processing."
        )

    raise ValueError(
        f"Unsupported StateSupervisionPeriodAdmissionReason value: {admission_reason}"
    )


def is_official_release(
    release_reason: Optional[StateIncarcerationPeriodReleaseReason],
) -> bool:
    """Returns whether or not the |release_reason| is considered an official end of incarceration, i.e. a release that
    terminates the continuous period of time spent incarcerated for a specific reason, not transfers or other unknown
    statuses resulting in facility changes."""
    if not release_reason:
        return False

    # An incarceration period that has one of these release reasons indicates the official end of that period of
    # incarceration
    official_release_types = [
        StateIncarcerationPeriodReleaseReason.COMMUTED,
        StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        StateIncarcerationPeriodReleaseReason.DEATH,
        StateIncarcerationPeriodReleaseReason.EXECUTION,
        StateIncarcerationPeriodReleaseReason.PARDONED,
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        # Someone may be released from temporary custody and immediately admitted to full custody. This is considered
        # an official release because it is an end to the period of temporary custody.
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        # Transfers to other jurisdictions are classified as official releases because the custodial authority is changing.
        StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
        StateIncarcerationPeriodReleaseReason.VACATED,
    ]

    non_official_release_types = [
        StateIncarcerationPeriodReleaseReason.COURT_ORDER,
        StateIncarcerationPeriodReleaseReason.ESCAPE,
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
        StateIncarcerationPeriodReleaseReason.TRANSFER,
        StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
        StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
        StateIncarcerationPeriodReleaseReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE,
        StateIncarcerationPeriodReleaseReason.RELEASE_FROM_WEEKEND_CONFINEMENT,
    ]

    if release_reason in official_release_types:
        return True
    if release_reason in non_official_release_types:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodReleaseReason value: {release_reason}"
    )


def release_reason_overrides_released_from_temporary_custody(
    release_reason: Optional[StateIncarcerationPeriodReleaseReason],
) -> bool:
    """RELEASED_FROM_TEMPORARY_CUSTODY is the expected release_reason for all periods of
    incarceration for which the admission_reason is TEMPORARY_CUSTODY. In certain
    cases, the release_reason contains more specific information about why the person
    was released from the period of temporary custody. This function returns whether
    we want to prioritize the existing release_reason over the standard
    RELEASED_FROM_TEMPORARY_CUSTODY.
    """
    # We want to prioritize these release reasons over RELEASED_FROM_TEMPORARY_CUSTODY
    prioritized_release_types = [
        StateIncarcerationPeriodReleaseReason.COMMUTED,
        StateIncarcerationPeriodReleaseReason.COURT_ORDER,
        StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
        StateIncarcerationPeriodReleaseReason.DEATH,
        StateIncarcerationPeriodReleaseReason.ESCAPE,
        StateIncarcerationPeriodReleaseReason.EXECUTION,
        StateIncarcerationPeriodReleaseReason.PARDONED,
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
        StateIncarcerationPeriodReleaseReason.VACATED,
        # If the release reason is already RELEASED_FROM_TEMPORARY_CUSTODY then
        # there's no reason to override it
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
    ]

    # If someone is being released from a period of TEMPORARY_CUSTODY or
    # PAROLE_BOARD_HOLD, we'd rather have the RELEASED_FROM_TEMPORARY_CUSTODY
    # release_reason over the following values
    non_prioritized_release_types = [
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
        StateIncarcerationPeriodReleaseReason.TRANSFER,
        StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
        StateIncarcerationPeriodReleaseReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE,
        StateIncarcerationPeriodReleaseReason.RELEASE_FROM_WEEKEND_CONFINEMENT,
    ]

    if release_reason in prioritized_release_types:
        return True
    if release_reason in non_prioritized_release_types:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodReleaseReason value: {release_reason}"
    )


# Commitment from supervision admissions for the following purposes of incarceration
# should always be classified as SANCTION_ADMISSION
SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES = [
    StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
    StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
]
