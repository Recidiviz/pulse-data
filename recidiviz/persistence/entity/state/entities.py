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
# ============================================================================
"""Domain logic entities used in the persistence layer for state data.

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate. This
allows these persistence layer objects additional flexibility that the SQL Alchemy
ORM objects can't provide.
"""

import datetime
from typing import List, Optional, TypeVar, Union

import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.state.state_agent import (
    StateAgentSubtype,
    StateAgentType,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
)
from recidiviz.common.constants.state.state_drug_screen import (
    StateDrugScreenResult,
    StateDrugScreenSampleType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodHousingUnitType,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_shared_enums import (
    StateActingBodyType,
    StateCustodialAuthority,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.date import DateRange, DurationMixin
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
    HasExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)

# **** Entity Types for convenience *****:
PeriodType = TypeVar(
    "PeriodType", bound=Union["StateSupervisionPeriod", "StateIncarcerationPeriod"]
)

# **** Entity ordering template *****:

# State Code

# Status

# Type

# Attributes
#   - When
#   - Where
#   - What
#   - Who

# Primary key - Only optional when hydrated in the parsing layer, before
# we have written this entity to the persistence layer

# Cross-entity relationships


@attr.s(eq=False, kw_only=True)
class StatePersonExternalId(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an external id associated with a particular StatePerson."""

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_external_id_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StatePersonAlias(Entity, BuildableAttr, DefaultableAttr):
    """Models an alias associated with a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    alias_type: Optional[StatePersonAliasType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StatePersonAliasType)
    )
    alias_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_alias_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StatePersonRace(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models a race associated with a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    race: Optional[StateRace] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateRace)
    )
    race_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_race_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StatePersonEthnicity(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models an ethnicity associated with a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    ethnicity: Optional[StateEthnicity] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateEthnicity)
    )
    ethnicity_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_ethnicity_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StatePerson(
    HasMultipleExternalIdsEntity[StatePersonExternalId],
    RootEntity,
    BuildableAttr,
    DefaultableAttr,
):
    """Models a StatePerson moving through the criminal justice system."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes

    #   - Where
    current_address: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    birthdate: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    gender: Optional[StateGender] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateGender)
    )
    gender_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # NOTE: This may change over time - we track these changes in history tables
    residency_status: Optional[StateResidencyStatus] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateResidencyStatus)
    )
    residency_status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    current_email_address: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    current_phone_number: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    external_ids: List["StatePersonExternalId"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    aliases: List["StatePersonAlias"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    races: List["StatePersonRace"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    ethnicities: List["StatePersonEthnicity"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    assessments: List["StateAssessment"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    incarceration_sentences: List["StateIncarcerationSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_sentences: List["StateSupervisionSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    incarceration_periods: List["StateIncarcerationPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_periods: List["StateSupervisionPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    program_assignments: List["StateProgramAssignment"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    incarceration_incidents: List["StateIncarcerationIncident"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_violations: List["StateSupervisionViolation"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_contacts: List["StateSupervisionContact"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    task_deadlines: List["StateTaskDeadline"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    drug_screens: List["StateDrugScreen"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    employment_periods: List["StateEmploymentPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    def get_external_ids(self) -> List["StatePersonExternalId"]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "person"


@attr.s(eq=False, kw_only=True)
class StateCharge(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCharge against a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateChargeStatus = attr.ib(
        validator=attr.validators.instance_of(StateChargeStatus)
    )  # non-nullable
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    # N/A

    # Attributes
    #   - When
    offense_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    date_charged: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    ncic_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # A code corresponding to actual sentencing terms within a jurisdiction
    statute: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    description: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    attempted: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    classification_type: Optional[StateChargeClassificationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateChargeClassificationType)
    )
    classification_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # E.g. 'A' for Class A, '1' for Level 1, etc
    classification_subtype: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    offense_type: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    is_violent: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_sex_offense: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_drug: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    counts: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    charge_notes: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    is_controlling: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    charging_entity: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    judge_full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    judge_external_id: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    judicial_district_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    charge_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    incarceration_sentences: List["StateIncarcerationSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_sentences: List["StateSupervisionSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateAssessment(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateAssessment conducted about a particular StatePerson."""

    #  State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A - Always "COMPLETED", for now

    # Type
    assessment_class: Optional[StateAssessmentClass] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentClass)
    )
    assessment_class_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    assessment_type: Optional[StateAssessmentType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentType)
    )
    assessment_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    assessment_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    assessment_score: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    assessment_level: Optional[StateAssessmentLevel] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentLevel)
    )
    assessment_level_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    assessment_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |conducting_agent| below

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    assessment_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships

    # Only optional when hydrated in the parsing layer, before we have written this
    # entity to the persistence layer
    person: Optional["StatePerson"] = attr.ib(default=None)
    conducting_agent: Optional["StateAgent"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionSentence(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a sentence for a supervisory period associated with one or more Charges
    against a StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateSentenceStatus = attr.ib(
        validator=attr.validators.instance_of(StateSentenceStatus)
    )  # non-nullable
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    supervision_type: Optional[StateSupervisionSentenceSupervisionType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionSentenceSupervisionType),
    )
    supervision_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When

    # The date the person was sentenced
    date_imposed: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date on which a sentence effectively begins being served, including any pre-trial jail detention time if applicable.
    effective_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_completion_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date the person finished serving this sentence
    completion_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    is_life: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Where
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    min_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    max_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    sentence_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    conditions: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_sentence_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    charges: List["StateCharge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    early_discharges: List["StateEarlyDischarge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateIncarcerationSentence(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a sentence for prison/jail time associated with one or more Charges
    against a StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateSentenceStatus = attr.ib(
        validator=attr.validators.instance_of(StateSentenceStatus)
    )  # non-nullable
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    incarceration_type: Optional[StateIncarcerationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationType)
    )
    incarceration_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When

    # The date the person was sentenced
    date_imposed: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date on which a sentence effectively begins being served, including any pre-trial jail detention time if applicable.
    effective_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_min_release_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_max_release_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    parole_eligibility_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date the person finished serving this sentence
    completion_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    # These will be None if is_life is true
    min_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    max_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    is_life: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_capital_punishment: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    parole_possible: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    initial_time_served_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    good_time_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    earned_time_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    sentence_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    conditions: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    incarceration_sentence_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    charges: List["StateCharge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    early_discharges: List["StateEarlyDischarge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateIncarcerationPeriod(
    HasExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """Models an uninterrupted period of time that a StatePerson is incarcerated at a
    single facility as a result of a particular sentence.
    """

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    incarceration_type: Optional[StateIncarcerationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationType)
    )
    incarceration_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    admission_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    release_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where the facility is located
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    facility: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    housing_unit: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    housing_unit_type: Optional[StateIncarcerationPeriodHousingUnitType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodHousingUnitType),
    )
    housing_unit_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    #   - What
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodAdmissionReason),
    )
    admission_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodReleaseReason),
    )
    release_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    custody_level: Optional[StateIncarcerationPeriodCustodyLevel] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodCustodyLevel),
    )
    custody_level_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSpecializedPurposeForIncarceration),
    )
    specialized_purpose_for_incarceration_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The type of government entity directly responsible for the person in this period
    # of incarceration. Not necessarily the decision making authority.
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateCustodialAuthority)
    )
    custodial_authority_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    incarceration_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days covered by the incarceration period. Since
        DateRange is never open, if the incarceration period is still active,
        then the exclusive upper bound of the range is set to tomorrow.
        """
        if not self.admission_date:
            raise ValueError(
                f"Expected start date for period {self.incarceration_period_id}, "
                "found None"
            )

        return DateRange.from_maybe_open_range(
            start_date=self.admission_date, end_date=self.release_date
        )

    @property
    def start_date_inclusive(self) -> Optional[datetime.date]:
        return self.admission_date

    @property
    def end_date_exclusive(self) -> Optional[datetime.date]:
        return self.release_date


@attr.s(eq=False, kw_only=True)
class StateSupervisionPeriod(
    HasExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """Models a distinct period of time that a StatePerson is under supervision as a
    result of a particular sentence."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodSupervisionType),
    )
    supervision_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    start_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    termination_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where this person is being supervised
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    supervision_site: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodAdmissionReason),
    )
    admission_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodTerminationReason),
    )
    termination_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    supervision_level: Optional[StateSupervisionLevel] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionLevel)
    )
    supervision_level_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The type of government entity directly responsible for the person on this period
    # of supervision. Not necessarily the decision making authority.
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateCustodialAuthority)
    )
    custodial_authority_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    conditions: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervising_officer: Optional["StateAgent"] = attr.ib(default=None)
    case_type_entries: List["StateSupervisionCaseTypeEntry"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days covered by the supervision period. Since
        DateRange is never open, if the supervision period is still active, then the
        exclusive upper bound of the range is set to tomorrow.
        """
        if not self.start_date:
            raise ValueError(
                f"Expected start date for period {self.supervision_period_id}, "
                "found None"
            )

        return DateRange.from_maybe_open_range(
            start_date=self.start_date, end_date=self.termination_date
        )

    @property
    def start_date_inclusive(self) -> Optional[datetime.date]:
        return self.start_date

    @property
    def end_date_exclusive(self) -> Optional[datetime.date]:
        return self.termination_date


@attr.s(eq=False, kw_only=True)
class StateSupervisionCaseTypeEntry(EnumEntity, BuildableAttr, DefaultableAttr):
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - What
    case_type: Optional[StateSupervisionCaseType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionCaseType)
    )
    case_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_case_type_entry_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_period: Optional["StateSupervisionPeriod"] = attr.ib(default=None)
    external_id: str = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateIncarcerationIncident(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a documented incident for a StatePerson while incarcerated."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A

    # Type
    incident_type: Optional[StateIncarcerationIncidentType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationIncidentType)
    )
    incident_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    incident_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    facility: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    location_within_facility: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    incident_details: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    incident_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    #  No fields

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    incarceration_incident_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    incarceration_incident_outcomes: List[
        "StateIncarcerationIncidentOutcome"
    ] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False, kw_only=True)
class StateIncarcerationIncidentOutcome(
    HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """Models the documented outcome in response to some StateIncarcerationIncident."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    outcome_type: Optional[StateIncarcerationIncidentOutcomeType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationIncidentOutcomeType),
    )
    outcome_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    date_effective: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    hearing_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    report_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    outcome_description: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    punishment_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    #   - Who
    # See |person| below

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    incarceration_incident_outcome_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    incarceration_incident: Optional["StateIncarcerationIncident"] = attr.ib(
        default=None
    )


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolationTypeEntry(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models a violation type associated with a particular
    StateSupervisionViolation."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    violation_type: Optional[StateSupervisionViolationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionViolationType)
    )
    violation_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_violation_type_entry_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_violation: Optional["StateSupervisionViolation"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolatedConditionEntry(
    EnumEntity, BuildableAttr, DefaultableAttr
):
    """Models a condition applied to a supervision sentence, whose violation may be
    recorded in a StateSupervisionViolation.
    """

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    # An enum corresponding to the condition
    condition: Optional[StateSupervisionViolatedConditionType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionViolatedConditionType),
    )

    # The most granular information from the state about the specific supervision condition that was violated
    condition_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_violated_condition_entry_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    supervision_violation: Optional["StateSupervisionViolation"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolation(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """
    Models a recorded instance where a StatePerson has violated one or more of the
    conditions of their StateSupervisionSentence.
    """

    # State Code
    # State that recorded this violation, not necessarily where the violation took place
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A

    # Attributes
    #   - When
    violation_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    # These should correspond to |conditions| in StateSupervisionPeriod
    is_violent: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_sex_offense: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_violation_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_violation_types: List["StateSupervisionViolationTypeEntry"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_violated_conditions: List[
        "StateSupervisionViolatedConditionEntry"
    ] = attr.ib(factory=list, validator=attr_validators.is_list)
    supervision_violation_responses: List[
        "StateSupervisionViolationResponse"
    ] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolationResponseDecisionEntry(
    EnumEntity, BuildableAttr, DefaultableAttr
):
    """Models the type of decision resulting from a response to a
    StateSupervisionViolation."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    decision: Optional[StateSupervisionViolationResponseDecision] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionViolationResponseDecision),
    )
    decision_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_violation_response_decision_entry_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_violation_response: Optional[
        "StateSupervisionViolationResponse"
    ] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolationResponse(
    HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """Models a response to a StateSupervisionViolation"""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A

    # Type
    response_type: Optional[StateSupervisionViolationResponseType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionViolationResponseType),
    )
    response_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    response_subtype: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    response_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    is_draft: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    # See SupervisionViolationResponders below
    deciding_body_type: Optional[
        StateSupervisionViolationResponseDecidingBodyType
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(
            StateSupervisionViolationResponseDecidingBodyType
        ),
    )
    deciding_body_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # See also |decision_agents| below

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_violation_response_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_violation: Optional["StateSupervisionViolation"] = attr.ib(default=None)
    supervision_violation_response_decisions: List[
        "StateSupervisionViolationResponseDecisionEntry"
    ] = attr.ib(factory=list, validator=attr_validators.is_list)
    decision_agents: List["StateAgent"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateAgent(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an agent working within a justice system."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    agent_type: StateAgentType = attr.ib(
        validator=attr.validators.instance_of(StateAgentType)
    )
    agent_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - What
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    agent_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Subtype
    agent_subtype: Optional[StateAgentSubtype] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAgentSubtype)
    )
    agent_subtype_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )


@attr.s(eq=False, kw_only=True)
class StateProgramAssignment(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an person's assignment to a particular program."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    participation_status: StateProgramAssignmentParticipationStatus = attr.ib(
        validator=attr.validators.instance_of(StateProgramAssignmentParticipationStatus)
    )  # non-nullable
    participation_status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    referral_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    start_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    discharge_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    program_id: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    program_location_id: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    referral_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    program_assignment_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    referring_agent: Optional["StateAgent"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateEarlyDischarge(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a person's sentenced-level early discharge requests."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    request_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    decision_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #  - What
    decision: Optional[StateEarlyDischargeDecision] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateEarlyDischargeDecision)
    )
    decision_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    decision_status: Optional[StateEarlyDischargeDecisionStatus] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateEarlyDischargeDecisionStatus),
    )
    decision_status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    deciding_body_type: Optional[StateActingBodyType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateActingBodyType)
    )
    deciding_body_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    requesting_body_type: Optional[StateActingBodyType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateActingBodyType)
    )
    requesting_body_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Where
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    early_discharge_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    # Should only be one of incarceration or supervision sentences on this.
    incarceration_sentence: Optional["StateIncarcerationSentence"] = attr.ib(
        default=None
    )
    supervision_sentence: Optional["StateSupervisionSentence"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionContact(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a person's contact with their supervising officer."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: Optional[StateSupervisionContactStatus] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactStatus)
    )
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    contact_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    contact_type: Optional[StateSupervisionContactType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactType)
    )
    contact_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - How
    contact_method: Optional[StateSupervisionContactMethod] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactMethod)
    )
    contact_method_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    contact_reason: Optional[StateSupervisionContactReason] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactReason)
    )
    contact_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    location: Optional[StateSupervisionContactLocation] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactLocation)
    )
    location_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    verified_employment: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    resulted_in_arrest: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_contact_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    contacted_agent: Optional["StateAgent"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateEmploymentPeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models information about a person's employment status during a certain period of
    time.
    """

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    employment_status: Optional[StateEmploymentPeriodEmploymentStatus] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateEmploymentPeriodEmploymentStatus),
    )
    employment_status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(default=None, validator=attr_validators.is_date)
    end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    last_verified_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    employer_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    job_title: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    end_reason: Optional[StateEmploymentPeriodEndReason] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateEmploymentPeriodEndReason)
    )
    end_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    employment_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateDrugScreen(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """The StateDrugScreen object represents information about a person's drug screen results for a given date."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    drug_screen_result: Optional[StateDrugScreenResult] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateDrugScreenResult),
    )
    drug_screen_result_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    drug_screen_date: datetime.date = attr.ib(
        default=None, validator=attr_validators.is_date
    )

    #   - What
    sample_type: Optional[StateDrugScreenSampleType] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateDrugScreenSampleType),
    )
    sample_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    drug_screen_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateTaskDeadline(Entity, BuildableAttr, DefaultableAttr):
    """The StateTaskDeadline object represents a single task that should be performed as
    part of someone’s supervision or incarceration term, along with an associated date
    that task can be started and/or a deadline when that task must be completed.
    """

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    eligible_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    due_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    update_datetime: datetime.datetime = attr.ib(
        default=None, validator=attr_validators.is_datetime
    )

    #   - What
    task_type: StateTaskType = attr.ib(
        validator=attr.validators.instance_of(StateTaskType),
    )
    task_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    task_subtype: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    task_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    task_deadline_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateStaffExternalId(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an external id associated with a particular StateStaff."""

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    external_id: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - What
    id_type: str = attr.ib(validator=attr_validators.is_str)

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_external_id_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    staff: Optional["StateStaff"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateStaff(
    HasMultipleExternalIdsEntity[StateStaffExternalId],
    RootEntity,
    BuildableAttr,
    DefaultableAttr,
):
    """Models a staff member working within a justice system."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - What
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    email: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    external_ids: List["StateStaffExternalId"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    role_periods: List["StateStaffRolePeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervisor_periods: List["StateStaffSupervisorPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    def get_external_ids(self) -> List[StateStaffExternalId]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "staff"


@attr.s(eq=False, kw_only=True)
class StateStaffRolePeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents information about a staff member’s role in the justice system during a
    particular period of time.
    """

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    external_id: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(default=None, validator=attr_validators.is_date)
    end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    role_type: StateStaffRoleType = attr.ib(
        validator=attr.validators.instance_of(StateStaffRoleType)
    )
    role_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    role_subtype: Optional[StateStaffRoleSubtype] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateStaffRoleSubtype)
    )
    role_subtype_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_role_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    staff: Optional["StateStaff"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateStaffSupervisorPeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents information about a staff member’s direct supervisor during a
    particular period of time.
    """

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    external_id: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(default=None, validator=attr_validators.is_date)
    end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    supervisor_staff_external_id: str = attr.ib(
        default=None, validator=attr_validators.is_str
    )
    supervisor_staff_external_id_type: str = attr.ib(
        default=None, validator=attr_validators.is_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_supervisor_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    staff: Optional["StateStaff"] = attr.ib(default=None)
