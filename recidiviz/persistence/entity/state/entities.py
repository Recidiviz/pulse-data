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

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate. This allows these persistence layer
objects additional flexibility that the SQL Alchemy ORM objects can't provide.
"""

import datetime
from typing import List, Optional, TypeVar, Union

import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.common.constants.state.shared_enums import (
    StateActingBodyType,
    StateCustodialAuthority,
)
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationFacilitySecurityLevel,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_parole_decision import (
    StateParoleDecisionOutcome,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
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
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange, DurationMixin
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
)
from recidiviz.persistence.entity.state.entity_deprecation_utils import (  # pylint: disable=unused-import
    validate_deprecated_entity_field_for_states,
)

# **** Entity Types for convenience *****:
SentenceType = TypeVar(
    "SentenceType", "StateSupervisionSentence", "StateIncarcerationSentence"
)

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

# Primary key - Only optional when hydrated in the data converter, before
# we have written this entity to the persistence layer

# Cross-entity relationships


@attr.s(eq=False, kw_only=True)
class StatePersonExternalId(Entity, BuildableAttr, DefaultableAttr):
    """Models an external id associated with a particular StatePerson."""

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    external_id: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - What
    id_type: str = attr.ib(validator=attr_validators.is_str)

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
    # TODO(#1905): Remove defaults for string fields
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
    race: Optional[Race] = attr.ib(default=None, validator=attr_validators.is_opt(Race))
    race_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
    ethnicity: Optional[Ethnicity] = attr.ib(
        default=None, validator=attr_validators.is_opt(Ethnicity)
    )
    ethnicity_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    person_ethnicity_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StatePerson(Entity, BuildableAttr, DefaultableAttr):
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
    # TODO(#7236): DEPRECATED - DO NOT ADD NEW USAGES
    birthdate_inferred_from_age: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    gender: Optional[Gender] = attr.ib(
        default=None, validator=attr_validators.is_opt(Gender)
    )
    gender_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # NOTE: This may change over time - we track these changes in history tables
    residency_status: Optional[ResidencyStatus] = attr.ib(
        default=None, validator=attr_validators.is_opt(ResidencyStatus)
    )

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
    program_assignments: List["StateProgramAssignment"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    sentence_groups: List["StateSentenceGroup"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervising_officer: Optional["StateAgent"] = attr.ib(default=None)

    # NOTE: Eventually we might have a relationship to objects holding pre-sentence information so we can track
    # encounters with the justice system that don't result in sentences.


@attr.s(eq=False, kw_only=True)
class StateCourtCase(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCourtCase associated with some set of StateCharges"""

    # State Code
    # Location of the court itself
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: Optional[StateCourtCaseStatus] = attr.ib(
        validator=attr_validators.is_opt(StateCourtCaseStatus)
    )
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    court_type: Optional[StateCourtType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateCourtType)
    )
    court_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    date_convicted: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    next_court_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # County where the court case took place
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # Area of jurisdictional coverage of the court which tried the case, may be the same as the county, the entire
    # state, or some jurisdiction out of the state.
    judicial_district_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    # TODO(#9072): DEPRECATED - DO NOT ADD NEW USAGES
    court_fee_dollars: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    #   - Who
    # See |judge| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    court_case_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    charges: List["StateCharge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    judge: Optional["StateAgent"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateCharge(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCharge against a particular StatePerson."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: ChargeStatus = attr.ib(
        validator=attr.validators.instance_of(ChargeStatus)
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    charge_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    court_case: Optional["StateCourtCase"] = attr.ib(default=None)

    incarceration_sentences: List["StateIncarcerationSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_sentences: List["StateSupervisionSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    fines: List["StateFine"] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False, kw_only=True)
class StateAssessment(ExternalIdEntity, BuildableAttr, DefaultableAttr):
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    assessment_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships

    # Only optional when hydrated in the data converter, before we have written this entity to the persistence layer
    person: Optional["StatePerson"] = attr.ib(default=None)
    conducting_agent: Optional["StateAgent"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSentenceGroup(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a group of related sentences, which may be served consecutively or concurrently."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    #   - Where
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Status
    # TODO(#1698): Look at Measures for Justice doc for methodology on how to calculate an aggregate sentence status
    #  from multiple sentence statuses.
    # This will be a composite of all the linked individual statuses
    status: StateSentenceStatus = attr.ib(
        default=None, validator=attr.validators.instance_of(StateSentenceStatus)
    )
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    # N/A

    # Attributes
    #   - When
    date_imposed: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # TODO(#1698): Consider including rollup projected completion dates?

    #   - What
    # See |supervision_sentences|, |incarceration_sentences|, and |fines| in entity relationships below for more of the
    # What.

    # Total length periods, either rolled up from individual sentences or directly reported from the ingested
    # source data
    min_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    max_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    is_life: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    sentence_group_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_sentences: List["StateSupervisionSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    incarceration_sentences: List["StateIncarcerationSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    fines: List["StateFine"] = attr.ib(factory=list, validator=attr_validators.is_list)
    # TODO(#1698): Add information about the time relationship between individual
    #  sentences (i.e. consecutive vs concurrent).


@attr.s(eq=False, kw_only=True)
class StateSupervisionSentence(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a sentence for a supervisory period associated with one or more Charges against a StatePerson."""

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
    # TODO(#2891): Make this of type StateSupervisionSentenceType (new type)
    supervision_type: Optional[StateSupervisionType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionType)
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

    # The date the person actually started serving this sentence
    start_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_completion_date: Optional[datetime.date] = attr.ib(
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
    min_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    max_length_days: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    #   - Who

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_sentence_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence_group: Optional["StateSentenceGroup"] = attr.ib(default=None)
    charges: List["StateCharge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    # NOTE: A person might have an incarceration period associated with a supervision sentence if they violate the
    # terms of the sentence and are sent back to prison.
    incarceration_periods: List["StateIncarcerationPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_periods: List["StateSupervisionPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    early_discharges: List["StateEarlyDischarge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateIncarcerationSentence(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a sentence for prison/jail time associated with one or more Charges against a StatePerson."""

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

    # The date the person actually started serving this sentence
    start_date: Optional[datetime.date] = attr.ib(
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

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_sentence_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence_group: Optional["StateSentenceGroup"] = attr.ib(default=None)
    charges: List["StateCharge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    incarceration_periods: List["StateIncarcerationPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_periods: List["StateSupervisionPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    early_discharges: List["StateEarlyDischarge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


# TODO(#9199): DEPRECATED - DO NOT ADD NEW USAGES
@attr.s(eq=False, kw_only=True)
class StateFine(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a fine that a StatePerson is sentenced to pay in association with a StateCharge."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateFineStatus = attr.ib(
        validator=attr.validators.instance_of(StateFineStatus)
    )  # non-nullable
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    # N/A

    # Attributes
    #   - When
    date_paid: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where this fine was issued
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    fine_dollars: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    fine_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    sentence_group: Optional["StateSentenceGroup"] = attr.ib(default=None)
    charges: List["StateCharge"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateIncarcerationPeriod(
    ExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """Models an uninterrupted period of time that a StatePerson is incarcerated at a single facility as a result of a
    particular sentence.
    """

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateIncarcerationPeriodStatus = attr.ib(
        validator=attr.validators.instance_of(StateIncarcerationPeriodStatus)
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

    #   - What
    facility_security_level: Optional[
        StateIncarcerationFacilitySecurityLevel
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationFacilitySecurityLevel),
    )
    facility_security_level_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodAdmissionReason),
    )
    admission_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    projected_release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodReleaseReason),
    )
    projected_release_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodReleaseReason),
    )
    release_reason_raw_text: Optional[str] = attr.ib(
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

    # The type of government entity directly responsible for the person in this period of incarceration.
    # Not necessarily the decision making authority.
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateCustodialAuthority)
    )
    custodial_authority_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    # NOTE: An incarceration period might count towards multiple sentences
    incarceration_sentences: List["StateIncarcerationSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_sentences: List["StateSupervisionSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    incarceration_incidents: List["StateIncarcerationIncident"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    parole_decisions: List["StateParoleDecision"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days covered by the incarceration period.  Since DateRange is never open,
        if the incarceration period is still active, then the exclusive upper bound of the range is set to tomorrow.
        """
        if not self.admission_date:
            raise ValueError(
                f"Expected start date for period {self.incarceration_period_id}, found None"
            )

        if (
            not self.release_date
            and self.status != StateIncarcerationPeriodStatus.IN_CUSTODY
        ):
            raise ValueError(
                "Unexpected missing release date. IP pre-processing is not properly"
                " setting missing dates."
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
    ExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """Models a distinct period of time that a StatePerson is under supervision as a result of a particular sentence."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    # TODO(#2891): DEPRECATED - use supervision_period_supervision_type instead. Delete this field once all existing
    #  users have migrated to the new field.
    supervision_type: Optional[StateSupervisionType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionType)
    )
    supervision_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    supervision_period_supervision_type: Optional[
        StateSupervisionPeriodSupervisionType
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodSupervisionType),
    )
    supervision_period_supervision_type_raw_text: Optional[str] = attr.ib(
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

    # The type of government entity directly responsible for the person on this period of supervision.
    # Not necessarily the decision making authority.
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervising_officer: Optional["StateAgent"] = attr.ib(default=None)

    # NOTE: A supervision period might count towards multiple sentences
    incarceration_sentences: List["StateIncarcerationSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_sentences: List["StateSupervisionSentence"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_violation_entries: List["StateSupervisionViolation"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    case_type_entries: List["StateSupervisionCaseTypeEntry"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    supervision_contacts: List["StateSupervisionContact"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days covered by the supervision period.  Since DateRange is never open,
        if the supervision period is still active, then the exclusive upper bound of the range is set to tomorrow.
        """
        if not self.start_date:
            raise ValueError(
                f"Expected start date for period {self.supervision_period_id}, found None"
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_case_type_entry_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_period: Optional["StateSupervisionPeriod"] = attr.ib(default=None)
    external_id: str = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateIncarcerationIncident(ExternalIdEntity, BuildableAttr, DefaultableAttr):
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

    #   - Who
    # See |responding_officer| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_incident_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    responding_officer: Optional["StateAgent"] = attr.ib(default=None)
    incarceration_period: Optional["StateIncarcerationPeriod"] = attr.ib(default=None)

    incarceration_incident_outcomes: List[
        "StateIncarcerationIncidentOutcome"
    ] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False, kw_only=True)
class StateIncarcerationIncidentOutcome(
    ExternalIdEntity, BuildableAttr, DefaultableAttr
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_incident_outcome_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    incarceration_incident: Optional["StateIncarcerationIncident"] = attr.ib(
        default=None
    )


@attr.s(eq=False, kw_only=True)
class StateParoleDecision(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Parole Decision for a StatePerson while under Incarceration."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    decision_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    corrective_action_deadline: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where the decision was made, if different from the county where this person is incarcerated.
    county_code: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    decision_outcome: Optional[StateParoleDecisionOutcome] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateParoleDecisionOutcome)
    )
    decision_outcome_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    decision_reasoning: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    corrective_action: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |decision_agents| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    parole_decision_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    incarceration_period: Optional["StateIncarcerationPeriod"] = attr.ib(default=None)
    decision_agents: List["StateAgent"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolationTypeEntry(EnumEntity, BuildableAttr, DefaultableAttr):
    """Models a violation type associated with a particular StateSupervisionViolation."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    violation_type: Optional[StateSupervisionViolationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionViolationType)
    )
    violation_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
    """Models a condition applied to a supervision sentence, whose violation may be recorded in a
    StateSupervisionViolation.
    """

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    # A string code corresponding to the condition - region specific.
    condition: str = attr.ib(validator=attr_validators.is_str)  # non-nullable

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violated_condition_entry_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    supervision_violation: Optional["StateSupervisionViolation"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionViolation(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """
    Models a recorded instance where a StatePerson has violated one or more of the conditions of their
    StateSupervisionSentence.
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violation_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    supervision_periods: List["StateSupervisionPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
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
    Entity, BuildableAttr, DefaultableAttr
):
    """Models the type of decision resulting from a response to a StateSupervisionViolation."""

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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
    ExternalIdEntity, BuildableAttr, DefaultableAttr
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
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
class StateAgent(ExternalIdEntity, BuildableAttr, DefaultableAttr):
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    agent_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )


@attr.s(eq=False, kw_only=True)
class StateProgramAssignment(ExternalIdEntity, BuildableAttr, DefaultableAttr):
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
    discharge_reason: Optional[StateProgramAssignmentDischargeReason] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateProgramAssignmentDischargeReason),
    )
    discharge_reason_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    referral_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    program_assignment_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    referring_agent: Optional["StateAgent"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateEarlyDischarge(ExternalIdEntity, BuildableAttr, DefaultableAttr):
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    early_discharge_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    # Should only be one of incarceration or supervision sentences on this.
    incarceration_sentence: Optional["StateIncarcerationSentence"] = attr.ib(
        default=None
    )
    supervision_sentence: Optional["StateIncarcerationSentence"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSupervisionContact(ExternalIdEntity, BuildableAttr, DefaultableAttr):
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

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_contact_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)
    contacted_agent: Optional["StateAgent"] = attr.ib(default=None)
    supervision_periods: List["StateSupervisionPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
