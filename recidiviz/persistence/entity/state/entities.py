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
from typing import List, Optional

import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
    StateChargeV2ClassificationType,
    StateChargeV2Status,
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
    StateIncarcerationIncidentSeverity,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodHousingUnitCategory,
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
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_person_housing_status_period import (
    StatePersonHousingStatusType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_shared_enums import (
    StateActingBodyType,
    StateCustodialAuthority,
)
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
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
    StateSupervisionPeriodLegalAuthority,
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
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateOrDateTime, DateRange, DurationMixin
from recidiviz.common.state_exempted_attrs_validator import state_exempted_validator
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
    HasExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
    UniqueConstraint,
)
from recidiviz.persistence.entity.state.entity_field_validators import (
    appears_with,
    parsing_opt_only,
)
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin

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
class StatePersonAddressPeriod(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models an address associated with a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    address_line_1: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_line_2: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_city: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_zip: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_county: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_start_date: datetime.date = attr.ib(
        default=None, validator=attr_validators.is_date
    )

    address_end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    address_is_verified: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    address_type: StatePersonAddressType = attr.ib(
        default=None, validator=attr.validators.instance_of(StatePersonAddressType)
    )

    address_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_address_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StatePersonHousingStatusPeriod(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models a housing status period associated with a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    housing_status_start_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    housing_status_end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    housing_status_type: StatePersonHousingStatusType = attr.ib(
        default=None,
        validator=attr.validators.instance_of(StatePersonHousingStatusType),
    )

    housing_status_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_housing_status_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="person_external_ids_unique_within_type_and_region",
                fields=[
                    "state_code",
                    "id_type",
                    "external_id",
                ],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StatePersonAlias(Entity, BuildableAttr, DefaultableAttr):
    """Models an alias associated with a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    full_name: str = attr.ib(validator=attr_validators.is_str)
    alias_type: Optional[StatePersonAliasType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StatePersonAliasType)
    )
    alias_type_raw_text: Optional[str] = attr.ib(
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
    race: StateRace = attr.ib(validator=attr.validators.instance_of(StateRace))
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
    ethnicity: StateEthnicity = attr.ib(
        validator=attr.validators.instance_of(StateEthnicity)
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
    sentences: List["StateSentence"] = attr.ib(
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
    address_periods: List["StatePersonAddressPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    housing_status_periods: List["StatePersonHousingStatusPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    sentence_groups: List["StateSentenceGroup"] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="charge_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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
    conducting_staff_external_id: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("conducting_staff_external_id_type"),
        ],
    )
    conducting_staff_external_id_type: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("conducting_staff_external_id"),
        ],
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    assessment_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships

    # Only optional when hydrated in the parsing layer, before we have written this
    # entity to the persistence layer
    person: Optional["StatePerson"] = attr.ib(default=None)


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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_sentence_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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
    housing_unit_category: Optional[
        StateIncarcerationPeriodHousingUnitCategory
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodHousingUnitCategory),
    )
    housing_unit_category_raw_text: Optional[str] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_period_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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
    start_date: datetime.date = attr.ib(validator=attr_validators.is_date)
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
    supervision_period_metadata: Optional[str] = attr.ib(
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

    legal_authority: Optional[StateSupervisionPeriodLegalAuthority] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodLegalAuthority),
    )
    legal_authority_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    conditions: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.
    supervising_officer_staff_external_id: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("supervising_officer_staff_external_id_type"),
        ],
    )
    supervising_officer_staff_external_id_type: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("supervising_officer_staff_external_id"),
        ],
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
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

    def __attrs_post_init__(self) -> None:
        if (
            self.supervising_officer_staff_external_id is None
            and self.supervising_officer_staff_external_id_type is not None
        ) or (
            self.supervising_officer_staff_external_id is not None
            and self.supervising_officer_staff_external_id_type is None
        ):
            raise ValueError(
                f"Found inconsistent supervising_officer_staff_external_id* fields for StateSupervisionPeriod with id {self.supervision_period_id}. "
                f"supervising_officer_staff_external_id: {self.supervising_officer_staff_external_id} supervising_officer_staff_external_id_type: {self.supervising_officer_staff_external_id_type}. "
                "Either both must be null or both must be nonnull."
            )


@attr.s(eq=False, kw_only=True)
class StateSupervisionCaseTypeEntry(EnumEntity, BuildableAttr, DefaultableAttr):
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - What
    case_type: StateSupervisionCaseType = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionCaseType)
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

    incident_severity: Optional[StateIncarcerationIncidentSeverity] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationIncidentSeverity),
    )
    incident_severity_raw_text: Optional[str] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_incident_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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
    projected_end_date: Optional[datetime.date] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_incident_outcome_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolationTypeEntry(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models a violation type associated with a particular
    StateSupervisionViolation."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    violation_type: StateSupervisionViolationType = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionViolationType)
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
    condition: StateSupervisionViolatedConditionType = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionViolatedConditionType),
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
    violation_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="supervision_violation_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolationResponseDecisionEntry(
    EnumEntity, BuildableAttr, DefaultableAttr
):
    """Models the type of decision resulting from a response to a
    StateSupervisionViolation."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    decision: StateSupervisionViolationResponseDecision = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionViolationResponseDecision)
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

    violation_response_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
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

    deciding_staff_external_id: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("deciding_staff_external_id_type"),
        ],
    )
    deciding_staff_external_id_type: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("deciding_staff_external_id"),
        ],
    )

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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="supervision_violation_response_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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
    referring_staff_external_id: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("referring_staff_external_id_type"),
        ],
    )
    referring_staff_external_id_type: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("referring_staff_external_id"),
        ],
    )
    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    program_assignment_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="program_assignment_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="early_discharge_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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

    supervision_contact_metadata: Optional[str] = attr.ib(
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
    contacting_staff_external_id: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("contacting_staff_external_id_type"),
        ],
    )
    contacting_staff_external_id_type: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("contacting_staff_external_id"),
        ],
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    supervision_contact_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="supervision_contact_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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
    employer_address: Optional[str] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="employment_period_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


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

    drug_screen_metadata: Optional[str] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="state_drug_screen_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateTaskDeadline(LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity):
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
        default=None, validator=attr_validators.is_not_future_datetime
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

    @classmethod
    def entity_tree_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="state_task_deadline_unique_per_person_update_date_type",
                fields=[
                    "state_code",
                    "task_type",
                    "task_subtype",
                    "task_metadata",
                    "update_datetime",
                ],
            )
        ]

    @property
    def ledger_partition_columns(self) -> List[str]:
        return ["task_type", "task_subtype", "task_metadata"]

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        """StateTaskDeadline ledger updates happen on update_datetime."""
        return self.update_datetime

    def __attrs_post_init__(self) -> None:
        """StateTaskDeadlines have an eligible date before a due date."""
        self.assert_datetime_less_than(self.eligible_date, self.due_date)


@attr.s(eq=False, kw_only=True)
class StateStaffExternalId(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an external id associated with a particular StateStaff."""

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_external_ids_unique_within_type_and_region",
                fields=["state_code", "id_type", "external_id"],
            )
        ]


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
    email: Optional[str] = attr.ib(
        default=None,
        validator=state_exempted_validator(
            attr_validators.is_opt_valid_email,
            exempted_states={
                StateCode.US_AR,  # TODO(#29046): Remove AR exemption after email fix
                StateCode.US_ME,  # TODO(#31928): Remove ME exemption after email fix
                StateCode.US_IX,  # TODO(#31930): Remove IX exemption after email fix
            },
        ),
    )

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
    location_periods: List["StateStaffLocationPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    caseload_type_periods: List["StateStaffCaseloadTypePeriod"] = attr.ib(
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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_role_periods_unique_within_region",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateStaffSupervisorPeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents information about a staff member’s direct supervisor during a
    particular period of time.
    """

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

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

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_supervisor_periods_unique_within_region",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateStaffLocationPeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents information about a period of time during which a staff member has
    a given assigned location.
    """

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(default=None, validator=attr_validators.is_date)
    end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    location_external_id: str = attr.ib(default=None, validator=attr_validators.is_str)

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_location_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    staff: Optional["StateStaff"] = attr.ib(default=None)

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_location_periods_unique_within_region",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateStaffCaseloadTypePeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents information about a staff member’s caseload type over a period."""

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # The caseload type that the officer supervises
    caseload_type: StateStaffCaseloadType = attr.ib(
        validator=attr_validators.is_opt(StateStaffCaseloadType),
    )
    caseload_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # Attributes
    #   - When
    # The beginning of the period where this officer had this type of specialized caseload
    start_date: datetime.date = attr.ib(default=None, validator=attr_validators.is_date)

    # The end of the period where this officer had this type of specialized caseload
    end_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_caseload_type_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    staff: Optional["StateStaff"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSentence(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents a formal judgement imposed by the court that details the form of time served
    in response to the set of charges for which someone was convicted.
    This table contains all the attributes we can observe about the sentence at the time of sentence imposition,
    all of which will remain static over the course of the sentence being served.
    """

    # State code of the state providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Unique internal identifier for a sentence
    # Primary key - Only optional when hydrated in the parsing layer,
    # before we have written this entity to the persistence layer
    sentence_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    sentence_group_external_id: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The date this sentence was imposed, e.g. the date of actual sentencing,
    # but not necessarily the date the person started serving the sentence.
    # This value is only optional if:
    #   - this is a paritially hydrated entity (not merged yet)
    #   - it has sentencing_authority = StateSentencingAuthority.OTHER_STATE
    imposed_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The amount of any time already served (in days) at time of sentence imposition,
    # to possibly be credited against the overall sentence duration.
    initial_time_served_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # The type of sentence *at imposition*, for example
    # INCARCERATION, PROBATION, etc.
    # *Only optional for parsing*
    sentence_type: Optional[StateSentenceType] = attr.ib(
        default=None,
        validator=parsing_opt_only(attr.validators.instance_of(StateSentenceType)),
    )

    # The class of authority imposing this sentence: COUNTY, STATE, etc.
    # A value of COUNTY means a county court imposed this sentence.
    # Only optional for parsing
    sentencing_authority: Optional[StateSentencingAuthority] = attr.ib(
        default=None,
        validator=parsing_opt_only(
            attr.validators.instance_of(StateSentencingAuthority)
        ),
    )
    sentencing_authority_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Raw text indicating whether a sentence is supervision/incarceration/etc
    sentence_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # True if this is sentence is for a life sentence
    is_life: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # True if this is sentence is for the death penalty
    is_capital_punishment: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # True if the person may be released to parole under the terms of this sentence
    # (only relevant to :INCARCERATION: sentence type)
    parole_possible: Optional[bool] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_bool,
    )

    # The code of the county under whose jurisdiction the sentence was imposed
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Identifier of the sentences to which this sentence is consecutive (external_id),
    # formatted as a string of comma-separated id’s. For instance, if sentence C has a
    # consecutive_sentence_id_array of [A, B], then both A and B must be completed before C can be served.
    # String must be parseable as a comma-separated list.
    parent_sentence_external_id_array: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )

    # A comma-separated list of conditions of this sentence which the person must follow to avoid a disciplinary
    # response. If this field is empty, there may still be applicable conditions that apply to someone's current term
    # of supervision/incarceration - either inherited from another ongoing sentence or the current supervision term.
    # (See conditions on StateSupervisionPeriod).
    conditions: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )

    # Additional metadata field with additional sentence attributes
    sentence_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    charges: List["StateChargeV2"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    sentence_status_snapshots: List["StateSentenceStatusSnapshot"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    sentence_lengths: List["StateSentenceLength"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    sentence_serving_periods: List["StateSentenceServingPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="sentence_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            ),
        ]


@attr.s(eq=False, kw_only=True)
class StateSentenceServingPeriod(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Represents the periods of time over which someone was actively serving a given sentence."""

    # Primary key that is optional before hydration
    sentence_serving_period_id: Optional[int] = attr.ib(
        validator=attr_validators.is_opt_int
    )
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # The date on which a person effectively begins serving a sentence,
    # including any pre-trial jail detention time if applicable.
    serving_start_date: datetime.date = attr.ib(validator=attr_validators.is_date)

    # The date on which a person finishes serving a given sentence.
    # This field can be null if the date has not been observed yet.
    # This should only be hydrated once we have actually observed the completion date of the sentence.
    # E.g., if a sentence record has a completion date on 2024-01-01, this sentence would have a NULL
    # completion_date until 2024-01-01, after which the completion date would reflect that date.
    # We expect that this date will not change after it has been hydrated,
    # except in cases where the data override is fixing an error.
    serving_end_date: Optional[datetime.date] = attr.ib(
        validator=attr_validators.is_opt_date
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence: Optional["StateSentence"] = attr.ib(default=None)

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        # TODO(#26249) investigate more constraints
        return [
            UniqueConstraint(
                name="state_sentence_serving_period_external_id_unique_within_state",
                fields=["state_code", "external_id"],
            ),
        ]


@attr.s(eq=False, kw_only=True)
class StateChargeV2(HasExternalIdEntity, BuildableAttr, DefaultableAttr):
    """A formal allegation of an offense with information about the context for how that allegation was brought forth.
    `date_charged` can be null for charges that have statuses like “DROPPED”
    `offense_date` can be null because of erroneous data from states

    TODO(#26240): Replace StateCharge with this entity
    """

    state_code: str = attr.ib(validator=attr_validators.is_str)

    status: StateChargeV2Status = attr.ib(
        validator=attr.validators.instance_of(StateChargeV2Status)
    )
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

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
    classification_type: Optional[StateChargeV2ClassificationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateChargeV2ClassificationType)
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
    charge_v2_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentences: List["StateSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="charge_v2_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class StateSentenceStatusSnapshot(
    LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """Represents a historical snapshot for when a given sentence had a given status."""

    state_code: str = attr.ib(validator=attr_validators.is_str)

    # The start of the period of time over which the sentence status is valid
    status_update_datetime: datetime.datetime = attr.ib(
        default=None, validator=attr_validators.is_not_future_datetime
    )
    # The status of a sentence
    status: StateSentenceStatus = attr.ib(
        validator=parsing_opt_only(attr.validators.instance_of(StateSentenceStatus))
    )

    # The raw text value of the status of the sentence
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    sentence_status_snapshot_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence: Optional["StateSentence"] = attr.ib(default=None)

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        return self.status_update_datetime


@attr.s(eq=False, kw_only=True)
class StateSentenceLength(LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity):
    """Represents a historical ledger of time attributes for a single sentence,
    including sentence length, accrued amount of days earned off, key dates, etc.

    The projected date fields for this entity can only be hydrated if the state provides
    sentence-specific estimates for individual sentences. Many states only provide projected
    dates for all sentences in a sentence group. If that is the case, hydrate the projected
    date fields on StateSentenceGroup.
    """

    state_code: str = attr.ib(validator=attr_validators.is_str)

    # The start of the period of time over which the set of all sentence length attributes are valid
    length_update_datetime: datetime.datetime = attr.ib(
        default=None, validator=attr_validators.is_not_future_datetime
    )
    # The minimum duration of this sentence in days
    sentence_length_days_min: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # The maximum duration of this sentence in days
    sentence_length_days_max: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # Any good time (in days) the person has credited against this sentence due to good conduct,
    # a.k.a. time off for good behavior, if applicable.
    good_time_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # Any earned time (in days) the person has credited against this sentence due to participation
    # in programming designed to reduce the likelihood of re-offense, if applicable.
    earned_time_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # The date on which a person is expected to become eligible for parole under the terms of this sentence
    parole_eligibility_date_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The date on which a person is projected to be released from incarceration to parole, if
    # they will be released to parole from the parent sentence.
    projected_parole_release_date_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The earliest date on which a person is projected to be released to liberty, if
    # they will be released to liberty from the parent sentence.
    projected_completion_date_min_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The latest date on which a person is projected to be released to liberty, if
    # they will be released to liberty from the parent sentence.
    projected_completion_date_max_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    sentence_length_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence: Optional["StateSentence"] = attr.ib(default=None)

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        return self.length_update_datetime

    # TODO(#27577) Better understand projected dates and enforce them properly.
    def __attrs_post_init__(self) -> None:
        """Ensures that parole eligibility is before potential completions and
        that projected completion dates are in the right order."""
        self.assert_datetime_less_than(
            self.projected_parole_release_date_external,
            self.projected_completion_date_min_external,
            before_description="projected parole release",
            after_description="projected minimum completion",
        )
        self.assert_datetime_less_than(
            self.projected_parole_release_date_external,
            self.projected_completion_date_max_external,
            before_description="projected parole release",
            after_description="projected maximum completion",
        )


@attr.s(eq=False, kw_only=True)
class StateSentenceGroup(BuildableAttr, DefaultableAttr, HasExternalIdEntity):
    """
    Represents a logical grouping of sentences that encompass an
    individual's interactions with a department of corrections.
    It begins with an individual's first sentence imposition and ends at liberty.
    This is a state agnostic term used by Recidiviz for a state
    specific administrative phenomena.
    """

    state_code: str = attr.ib(validator=attr_validators.is_str)
    # Unique internal identifier for a sentence group
    # Primary key - Only optional when hydrated in the parsing layer,
    # before we have written this entity to the persistence layer
    sentence_group_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence_group_lengths: List["StateSentenceGroupLength"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateSentenceGroupLength(
    LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """Represents a historical ledger of attributes relating to a state designated group of sentences."""

    state_code: str = attr.ib(validator=attr_validators.is_str)

    # The date when all sentence term attributes are updated
    group_update_datetime: datetime.datetime = attr.ib(
        default=None, validator=attr_validators.is_not_future_datetime
    )

    # The date on which a person is expected to become eligible for parole under the terms of this sentence
    parole_eligibility_date_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The date on which a person is projected to be released from incarceration to parole
    projected_parole_release_date_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The earliest date on which a person is projected to be released to liberty after having completed
    # all sentences in the term.
    projected_full_term_release_date_min_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The latest date on which a person is projected to be released to liberty after having completed
    # all sentences in the term.
    projected_full_term_release_date_max_external: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # Primary key - Only optional when parsing, before we have written this entity to the persistence layer.
    # A unique identifier for the collection of sentences defining a continuous period of time served
    # in the criminal justice system.
    sentence_group_length_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence_group: Optional["StateSentenceGroup"] = attr.ib(default=None)

    # TODO(#27577) Better understand projected dates and enforce them properly.
    def __attrs_post_init__(self) -> None:
        """Ensures that parole eligibility is before potential completions and
        that projected completion dates are in the right order."""
        self.assert_datetime_less_than(
            self.projected_parole_release_date_external,
            self.projected_full_term_release_date_min_external,
            before_description="projected parole release",
            after_description="projected minimum full term release",
        )
        self.assert_datetime_less_than(
            self.projected_parole_release_date_external,
            self.projected_full_term_release_date_max_external,
            before_description="projected parole release",
            after_description="projected maximum full term release",
        )

    @property
    def ledger_partition_columns(self) -> List[str]:
        return []

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        return self.group_update_datetime
