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

from typing import Optional, List, TypeVar

import datetime
import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
    StateAssessmentLevel,
)

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import (
    Gender,
    Race,
    Ethnicity,
    ResidencyStatus
)
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType

from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType)
from recidiviz.common.constants.state.state_incarceration import (
    StateIncarcerationType,
)
from recidiviz.common.constants.state.state_incarceration_incident import \
    StateIncarcerationIncidentType, StateIncarcerationIncidentOutcomeType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationFacilitySecurityLevel,
    StateSpecializedPurposeForIncarceration)

from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
)
from recidiviz.common.constants.state.state_parole_decision import \
    StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_person_alias import \
    StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import \
    StateProgramAssignmentParticipationStatus, \
    StateProgramAssignmentDischargeReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_early_discharge import StateEarlyDischargeDecision, \
    StateEarlyDischargeDecisionStatus
from recidiviz.common.constants.state.shared_enums import StateActingBodyType, StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision import (
    StateSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactStatus, \
    StateSupervisionContactType, StateSupervisionContactReason, StateSupervisionContactLocation
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionLevel, StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import \
    StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecidingBodyType, \
    StateSupervisionViolationResponseType

from recidiviz.common.date import DateRange, DurationMixin
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity

# **** Entity Types for convenience *****:
SentenceType = TypeVar('SentenceType', 'StateSupervisionSentence', 'StateIncarcerationSentence')
PeriodType = TypeVar('PeriodType', 'StateSupervisionPeriod', 'StateIncarcerationPeriod')

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


@attr.s(eq=False)
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
    person_external_id_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(eq=False)
class StatePersonAlias(Entity, BuildableAttr, DefaultableAttr):
    """Models an alias associated with a particular StatePerson."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    alias_type: Optional[StatePersonAliasType] = attr.ib()
    alias_type_raw_text: Optional[str] = attr.ib()
    # TODO(#1905): Remove defaults for string fields
    full_name: Optional[str] = attr.ib(default=None)

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    person_alias_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(eq=False)
class StatePersonRace(Entity, BuildableAttr, DefaultableAttr):
    """Models a race associated with a particular StatePerson."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    race: Optional[Race] = attr.ib(default=None, validator=attr_validators.is_opt(Race))
    race_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    person_race_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(eq=False)
class StatePersonEthnicity(Entity, BuildableAttr, DefaultableAttr):
    """Models an ethnicity associated with a particular StatePerson."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    ethnicity: Optional[Ethnicity] = attr.ib(default=None, validator=attr_validators.is_opt(Ethnicity))
    ethnicity_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    person_ethnicity_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(eq=False)
class StatePerson(Entity, BuildableAttr, DefaultableAttr):
    """Models a StatePerson moving through the criminal justice system."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes

    #   - Where
    current_address: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - What
    full_name: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    birthdate: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    birthdate_inferred_from_age: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    gender: Optional[Gender] = attr.ib(default=None, validator=attr_validators.is_opt(Gender))
    gender_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # NOTE: This may change over time - we track these changes in history tables
    residency_status: Optional[ResidencyStatus] = attr.ib(default=None,
                                                          validator=attr_validators.is_opt(ResidencyStatus))

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    person_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    external_ids: List['StatePersonExternalId'] = attr.ib(factory=list, validator=attr_validators.is_list)
    aliases: List['StatePersonAlias'] = attr.ib(factory=list, validator=attr_validators.is_list)
    races: List['StatePersonRace'] = attr.ib(factory=list, validator=attr_validators.is_list)
    ethnicities: List['StatePersonEthnicity'] = attr.ib(factory=list, validator=attr_validators.is_list)
    assessments: List['StateAssessment'] = attr.ib(factory=list, validator=attr_validators.is_list)
    program_assignments: List['StateProgramAssignment'] = attr.ib(factory=list, validator=attr_validators.is_list)
    sentence_groups: List['StateSentenceGroup'] = attr.ib(factory=list, validator=attr_validators.is_list)
    supervising_officer: Optional['StateAgent'] = attr.ib(default=None)

    # NOTE: Eventually we might have a relationship to objects holding pre-sentence information so we can track
    # encounters with the justice system that don't result in sentences.


@attr.s(eq=False)
class StateBond(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateBond associated with a particular StateCharge."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: BondStatus = attr.ib(validator=attr.validators.instance_of(BondStatus))  # non-nullable
    status_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Type
    bond_type: Optional[BondType] = attr.ib(default=None, validator=attr_validators.is_opt(BondType))
    bond_type_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Attributes
    #   - When
    date_paid: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - Where
    county_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - What
    amount_dollars: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    #   - Who
    bond_agent: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    bond_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False)
class StateCourtCase(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCourtCase associated with some set of StateCharges"""
    # State Code
    # Location of the court itself
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: Optional[StateCourtCaseStatus] = attr.ib(validator=attr_validators.is_opt(StateCourtCaseStatus))
    status_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Type
    court_type: Optional[StateCourtType] = attr.ib(default=None, validator=attr_validators.is_opt(StateCourtType))
    court_type_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Attributes
    #   - When
    date_convicted: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    next_court_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - Where
    # County where the court case took place
    county_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    # Area of jurisdictional coverage of the court which tried the case, may be the same as the county, the entire
    # state, or some jurisdiction out of the state.
    judicial_district_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - What
    court_fee_dollars: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    #   - Who
    # See |judge| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    court_case_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list, validator=attr_validators.is_list)
    judge: Optional['StateAgent'] = attr.ib(default=None)


@attr.s(eq=False)
class StateCharge(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCharge against a particular StatePerson."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: ChargeStatus = attr.ib(validator=attr.validators.instance_of(ChargeStatus))  # non-nullable
    status_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Type
    # N/A

    # Attributes
    #   - When
    offense_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    date_charged: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - Where
    county_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - What
    ncic_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # A code corresponding to actual sentencing terms within a jurisdiction
    statute: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    description: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    attempted: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)
    classification_type: Optional[StateChargeClassificationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateChargeClassificationType))
    classification_type_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    # E.g. 'A' for Class A, '1' for Level 1, etc
    classification_subtype: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    offense_type: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    is_violent: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    counts: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    charge_notes: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    is_controlling: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    #   - Who
    charging_entity: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    charge_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    court_case: Optional['StateCourtCase'] = attr.ib(default=None)
    bond: Optional['StateBond'] = attr.ib(default=None)

    incarceration_sentences: List['StateIncarcerationSentence'] = attr.ib(factory=list,
                                                                          validator=attr_validators.is_list)
    supervision_sentences: List['StateSupervisionSentence'] = attr.ib(factory=list, validator=attr_validators.is_list)
    fines: List['StateFine'] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False)
class StateAssessment(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateAssessment conducted about a particular StatePerson."""
    #  State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A - Always "COMPLETED", for now

    # Type
    assessment_class: Optional[StateAssessmentClass] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentClass))
    assessment_class_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None,
                                                             validator=attr_validators.is_opt(StateAssessmentType))
    assessment_type_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Attributes
    #   - When
    assessment_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - What
    assessment_score: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    assessment_level: Optional[StateAssessmentLevel] = attr.ib(default=None,
                                                               validator=attr_validators.is_opt(StateAssessmentLevel))
    assessment_level_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    assessment_metadata: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - Who
    # See |conducting_agent| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    assessment_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships

    # Only optional when hydrated in the data converter, before we have written this entity to the persistence layer
    person: Optional['StatePerson'] = attr.ib(default=None)
    incarceration_period: Optional['StateIncarcerationPeriod'] = attr.ib(default=None)
    supervision_period: Optional['StateSupervisionPeriod'] = attr.ib(default=None)
    conducting_agent: Optional['StateAgent'] = attr.ib(default=None)


@attr.s(eq=False)
class StateSentenceGroup(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a group of related sentences, which may be served consecutively or concurrently."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    #   - Where
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Status
    # TODO(#1698): Look at Measures for Justice doc for methodology on how to calculate an aggregate sentence status
    #  from multiple sentence statuses.
    # This will be a composite of all the linked individual statuses
    status: StateSentenceStatus = attr.ib(default=None, validator=attr.validators.instance_of(StateSentenceStatus))
    status_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Type
    # N/A

    # Attributes
    #   - When
    date_imposed: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    # TODO(#1698): Consider including rollup projected completion dates?

    #   - What
    # See |supervision_sentences|, |incarceration_sentences|, and |fines| in entity relationships below for more of the
    # What.

    # Total length periods, either rolled up from individual sentences or directly reported from the ingested
    # source data
    min_length_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    max_length_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # TODO(#2668): Remove this column - can be derived from incarceration
    #  sentences.
    is_life: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    sentence_group_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_sentences: List['StateSupervisionSentence'] = attr.ib(factory=list,
                                                                      validator=attr_validators.is_list)
    incarceration_sentences: List['StateIncarcerationSentence'] = attr.ib(factory=list,
                                                                          validator=attr_validators.is_list)
    fines: List['StateFine'] = attr.ib(factory=list, validator=attr_validators.is_list)
    # TODO(#1698): Add information about the time relationship between individual
    #  sentences (i.e. consecutive vs concurrent).


@attr.s(eq=False)
class StateSupervisionSentence(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a sentence for a supervisory period associated with one or more Charges against a StatePerson."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateSentenceStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    # TODO(#2891): Make this of type StateSupervisionSentenceType (new type)
    supervision_type: Optional[StateSupervisionType] = attr.ib()
    supervision_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When

    # The date the person was sentenced
    date_imposed: Optional[datetime.date] = attr.ib()

    # The date the person actually started serving this sentence
    start_date: Optional[datetime.date] = attr.ib()
    projected_completion_date: Optional[datetime.date] = attr.ib()

    # The date the person finished serving this sentence
    completion_date: Optional[datetime.date] = attr.ib()

    #   - Where
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib()

    #   - What
    min_length_days: Optional[int] = attr.ib()
    max_length_days: Optional[int] = attr.ib()

    #   - Who

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_sentence_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    sentence_group: Optional['StateSentenceGroup'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list)

    # NOTE: A person might have an incarceration period associated with a supervision sentence if they violate the
    # terms of the sentence and are sent back to prison.
    incarceration_periods: List['StateIncarcerationPeriod'] = attr.ib(factory=list)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list)
    early_discharges: List['StateEarlyDischarge'] = attr.ib(factory=list)


@attr.s(eq=False)
class StateIncarcerationSentence(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a sentence for prison/jail time associated with one or more Charges against a StatePerson."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateSentenceStatus = attr.ib(validator=attr.validators.instance_of(StateSentenceStatus))  # non-nullable
    status_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Type
    incarceration_type: Optional[StateIncarcerationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationType))
    incarceration_type_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Attributes
    #   - When

    # The date the person was sentenced
    date_imposed: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    # The date the person actually started serving this sentence
    start_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    projected_min_release_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    projected_max_release_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)
    parole_eligibility_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    # The date the person finished serving this sentence
    completion_date: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - Where
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - What
    # These will be None if is_life is true
    min_length_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    max_length_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    is_life: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)
    is_capital_punishment: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    parole_possible: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)
    initial_time_served_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    good_time_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    earned_time_days: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_sentence_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    sentence_group: Optional['StateSentenceGroup'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list, validator=attr_validators.is_list)

    incarceration_periods: List['StateIncarcerationPeriod'] = attr.ib(factory=list, validator=attr_validators.is_list)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list, validator=attr_validators.is_list)
    early_discharges: List['StateEarlyDischarge'] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False)
class StateFine(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a fine that a StatePerson is sentenced to pay in association with a StateCharge."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateFineStatus = attr.ib(validator=attr.validators.instance_of(StateFineStatus))  # non-nullable
    status_raw_text: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Type
    # N/A

    # Attributes
    #   - When
    date_paid: Optional[datetime.date] = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - Where
    # The county where this fine was issued
    county_code: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - What
    fine_dollars: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    fine_id: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    sentence_group: Optional['StateSentenceGroup'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list, validator=attr_validators.is_list)


@attr.s(eq=False)
class StateIncarcerationPeriod(ExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin):
    """Models an uninterrupted period of time that a StatePerson is incarcerated at a single facility as a result of a
    particular sentence.
    """
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateIncarcerationPeriodStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    incarceration_type: Optional[StateIncarcerationType] = attr.ib()
    incarceration_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    admission_date: Optional[datetime.date] = attr.ib()
    release_date: Optional[datetime.date] = attr.ib()

    #   - Where
    # The county where the facility is located
    county_code: Optional[str] = attr.ib()

    facility: Optional[str] = attr.ib()
    housing_unit: Optional[str] = attr.ib()

    #   - What
    facility_security_level: Optional[StateIncarcerationFacilitySecurityLevel] = attr.ib()
    facility_security_level_raw_text: Optional[str] = attr.ib()

    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib()
    admission_reason_raw_text: Optional[str] = attr.ib()

    projected_release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib()
    projected_release_reason_raw_text: Optional[str] = attr.ib()

    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib()
    release_reason_raw_text: Optional[str] = attr.ib()

    specialized_purpose_for_incarceration: Optional[StateSpecializedPurposeForIncarceration] = attr.ib()
    specialized_purpose_for_incarceration_raw_text: Optional[str] = attr.ib()

    # The type of government entity directly responsible for the person in this period of incarceration.
    # Not necessarily the decision making authority.
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib()
    custodial_authority_raw_text: Optional[str] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_period_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)

    # NOTE: An incarceration period might count towards multiple sentences
    incarceration_sentences: List['StateIncarcerationSentence'] = attr.ib(factory=list)
    supervision_sentences: List['StateSupervisionSentence'] = attr.ib(factory=list)

    incarceration_incidents: List['StateIncarcerationIncident'] = attr.ib(factory=list)
    parole_decisions: List['StateParoleDecision'] = attr.ib(factory=list)
    assessments: List['StateAssessment'] = attr.ib(factory=list)
    program_assignments: List['StateProgramAssignment'] = attr.ib(factory=list)

    # When the admission reason is PROBATION_REVOCATION or PAROLE_REVOCATION, this is the object with info about the
    # violation/hearing that resulted in the revocation
    source_supervision_violation_response: Optional['StateSupervisionViolationResponse'] = attr.ib(default=None)

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days covered by the incarceration period.  Since DateRange is never open,
        if the incarceration period is still active, then the exclusive upper bound of the range is set to tomorrow.
        """
        if not self.admission_date:
            raise ValueError(
                f'Expected start date for period {self.incarceration_period_id}, found None')

        if (not self.release_date and self.status != StateIncarcerationPeriodStatus.IN_CUSTODY):
            raise ValueError("Unexpected missing release date. _infer_missing_dates_and_statuses is not properly"
                                " setting missing dates.")

        return DateRange.from_maybe_open_range(start_date=self.admission_date, end_date=self.release_date)


@attr.s(eq=False)
class StateSupervisionPeriod(ExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin):
    """Models a distinct period of time that a StatePerson is under supervision as a result of a particular sentence."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: StateSupervisionPeriodStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    # TODO(#2891): DEPRECATED - use supervision_period_supervision_type instead. Delete this field once all existing
    #  users have migrated to the new field.
    supervision_type: Optional[StateSupervisionType] = attr.ib()
    supervision_type_raw_text: Optional[str] = attr.ib()
    supervision_period_supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib()
    supervision_period_supervision_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    start_date: Optional[datetime.date] = attr.ib()
    termination_date: Optional[datetime.date] = attr.ib()

    #   - Where
    # The county where this person is being supervised
    county_code: Optional[str] = attr.ib()

    supervision_site: Optional[str] = attr.ib()

    #   - What
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason] = attr.ib()
    admission_reason_raw_text: Optional[str] = attr.ib()

    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = attr.ib()
    termination_reason_raw_text: Optional[str] = attr.ib()

    supervision_level: Optional[StateSupervisionLevel] = attr.ib()
    supervision_level_raw_text: Optional[str] = attr.ib()

    # The type of government entity directly responsible for the person on this period of supervision.
    # Not necessarily the decision making authority.
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib()
    custodial_authority_raw_text: Optional[str] = attr.ib()

    conditions: Optional[str] = attr.ib(default=None)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_period_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervising_officer: Optional['StateAgent'] = attr.ib(default=None)
    program_assignments: List['StateProgramAssignment'] = attr.ib(factory=list)

    # NOTE: A supervision period might count towards multiple sentences
    incarceration_sentences: List['StateIncarcerationSentence'] = attr.ib(factory=list)
    supervision_sentences: List['StateSupervisionSentence'] = attr.ib(factory=list)
    supervision_violation_entries: List['StateSupervisionViolation'] = attr.ib(factory=list)
    assessments: List['StateAssessment'] = attr.ib(factory=list)
    case_type_entries: List['StateSupervisionCaseTypeEntry'] = attr.ib(factory=list)
    supervision_contacts: List['StateSupervisionContact'] = attr.ib(factory=list)

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days covered by the supervision period.  Since DateRange is never open,
        if the supervision period is still active, then the exclusive upper bound of the range is set to tomorrow.
        """
        if not self.start_date:
            raise ValueError(f'Expected start date for period {self.supervision_period_id}, found None')

        return DateRange.from_maybe_open_range(start_date=self.start_date, end_date=self.termination_date)


@attr.s(eq=False)
class StateSupervisionCaseTypeEntry(Entity, BuildableAttr, DefaultableAttr):
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - What
    case_type: Optional[StateSupervisionCaseType] = attr.ib()
    case_type_raw_text: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_case_type_entry_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_period: Optional['StateSupervisionPeriod'] = attr.ib(default=None)
    external_id: str = attr.ib(default=None)


@attr.s(eq=False)
class StateIncarcerationIncident(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a documented incident for a StatePerson while incarcerated."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A

    # Type
    incident_type: Optional[StateIncarcerationIncidentType] = attr.ib()
    incident_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    incident_date: Optional[datetime.date] = attr.ib()

    #   - Where
    facility: Optional[str] = attr.ib()
    location_within_facility: Optional[str] = attr.ib()

    #   - What
    incident_details: Optional[str] = attr.ib()

    #   - Who
    # See |responding_officer| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_incident_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    responding_officer: Optional['StateAgent'] = attr.ib(default=None)
    incarceration_period: Optional['StateIncarcerationPeriod'] = attr.ib(default=None)

    incarceration_incident_outcomes: List['StateIncarcerationIncidentOutcome'] = attr.ib(factory=list)


@attr.s(eq=False)
class StateIncarcerationIncidentOutcome(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    outcome_type: Optional[StateIncarcerationIncidentOutcomeType] = attr.ib()
    outcome_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    date_effective: Optional[datetime.date] = attr.ib()
    hearing_date: Optional[datetime.date] = attr.ib()
    report_date: Optional[datetime.date] = attr.ib()

    #   - What
    outcome_description: Optional[str] = attr.ib()
    punishment_length_days: Optional[int] = attr.ib()

    #   - Who
    # See |person| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    incarceration_incident_outcome_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    incarceration_incident: Optional['StateIncarcerationIncident'] = attr.ib(default=None)


@attr.s(eq=False)
class StateParoleDecision(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Parole Decision for a StatePerson while under Incarceration."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    decision_date: Optional[datetime.date] = attr.ib()
    corrective_action_deadline: Optional[datetime.date] = attr.ib()

    #   - Where
    # The county where the decision was made, if different from the county where this person is incarcerated.
    county_code: Optional[str] = attr.ib()

    #   - What
    decision_outcome: Optional[StateParoleDecisionOutcome] = attr.ib()
    decision_outcome_raw_text: Optional[str] = attr.ib()
    decision_reasoning: Optional[str] = attr.ib()
    corrective_action: Optional[str] = attr.ib()

    #   - Who
    # See |decision_agents| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    parole_decision_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    incarceration_period: Optional['StateIncarcerationPeriod'] = attr.ib(default=None)
    decision_agents: List['StateAgent'] = attr.ib(factory=list)


@attr.s(eq=False)
class StateSupervisionViolationTypeEntry(Entity, BuildableAttr, DefaultableAttr):
    """Models a violation type associated with a particular StateSupervisionViolation."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    violation_type: Optional[StateSupervisionViolationType] = attr.ib()
    violation_type_raw_text: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violation_type_entry_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_violation: Optional['StateSupervisionViolation'] = attr.ib(default=None)


@attr.s(eq=False)
class StateSupervisionViolatedConditionEntry(Entity, BuildableAttr, DefaultableAttr):
    """Models a condition applied to a supervision sentence, whose violation may be recorded in a
    StateSupervisionViolation.
    """
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    # A string code corresponding to the condition - region specific.
    condition: str = attr.ib()  # non-nullable

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violated_condition_entry_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)

    supervision_violation: Optional['StateSupervisionViolation'] = attr.ib(default=None)


@attr.s(eq=False)
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

    # Type
    # TODO(#2668): DEPRECATED - DELETE IN FOLLOW-UP PR
    violation_type: Optional[StateSupervisionViolationType] = attr.ib()
    violation_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    violation_date: Optional[datetime.date] = attr.ib()

    #   - What
    # These should correspond to |conditions| in StateSupervisionPeriod
    is_violent: Optional[bool] = attr.ib()
    is_sex_offense: Optional[bool] = attr.ib()

    # TODO(#2668): DEPRECATED - DELETE IN FOLLOW-UP PR
    violated_conditions: Optional[str] = attr.ib(default=None)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violation_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list)
    supervision_violation_types: List['StateSupervisionViolationTypeEntry'] = attr.ib(factory=list)
    supervision_violated_conditions: List['StateSupervisionViolatedConditionEntry'] = attr.ib(factory=list)
    supervision_violation_responses: List['StateSupervisionViolationResponse'] = attr.ib(factory=list)


@attr.s(eq=False)
class StateSupervisionViolationResponseDecisionEntry(Entity, BuildableAttr, DefaultableAttr):
    """Models the type of decision resulting from a response to a StateSupervisionViolation."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    decision: Optional[StateSupervisionViolationResponseDecision] = attr.ib()
    decision_raw_text: Optional[str] = attr.ib()

    # Only nonnull if one of the decisions is REVOCATION
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] = attr.ib()
    revocation_type_raw_text: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violation_response_decision_entry_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_violation_response: Optional['StateSupervisionViolationResponse'] = attr.ib(default=None)


@attr.s(eq=False)
class StateSupervisionViolationResponse(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a response to a StateSupervisionViolation"""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    # N/A

    # Type
    response_type: Optional[StateSupervisionViolationResponseType] = attr.ib()
    response_type_raw_text: Optional[str] = attr.ib()
    response_subtype: Optional[str] = attr.ib()

    # Attributes
    #   - When
    response_date: Optional[datetime.date] = attr.ib()

    #   - What
    # TODO(#2668): DEPRECATED - DELETE IN FOLLOW-UP PR
    decision: Optional[StateSupervisionViolationResponseDecision] = attr.ib()
    decision_raw_text: Optional[str] = attr.ib()

    # Only nonnull if one of the decisions is REVOCATION
    # TODO(#2668): DEPRECATED - DELETE IN FOLLOW-UP PR
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] = attr.ib()
    revocation_type_raw_text: Optional[str] = attr.ib()
    is_draft: Optional[bool] = attr.ib()

    #   - Who
    # See SupervisionViolationResponders below
    deciding_body_type: Optional[StateSupervisionViolationResponseDecidingBodyType] = attr.ib()
    deciding_body_type_raw_text: Optional[str] = attr.ib()
    # See also |decision_agents| below

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_violation_response_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_violation: Optional['StateSupervisionViolation'] = attr.ib(default=None)
    supervision_violation_response_decisions: List['StateSupervisionViolationResponseDecisionEntry'] = \
        attr.ib(factory=list)
    decision_agents: List['StateAgent'] = attr.ib(factory=list)


@attr.s(eq=False)
class StateAgent(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an agent working within a justice system."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Type
    agent_type: StateAgentType = attr.ib()
    agent_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - What
    full_name: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    agent_id: Optional[int] = attr.ib(default=None)


@attr.s(eq=False)
class StateProgramAssignment(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an person's assignment to a particular program."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    participation_status: StateProgramAssignmentParticipationStatus = attr.ib()  # non-nullable
    participation_status_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    referral_date: Optional[datetime.date] = attr.ib()
    start_date: Optional[datetime.date] = attr.ib()
    discharge_date: Optional[datetime.date] = attr.ib()

    #   - What
    program_id: Optional[str] = attr.ib()
    program_location_id: Optional[str] = attr.ib()
    discharge_reason: Optional[StateProgramAssignmentDischargeReason] = attr.ib()
    discharge_reason_raw_text: Optional[str] = attr.ib()
    referral_metadata: Optional[str] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    program_assignment_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    referring_agent: Optional['StateAgent'] = attr.ib(default=None)
    incarceration_periods: List['StateIncarcerationPeriod'] = attr.ib(factory=list)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list)


@attr.s(eq=False)
class StateEarlyDischarge(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a person's sentenced-level early discharge requests."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes
    #   - When
    request_date: Optional[datetime.date] = attr.ib()
    decision_date: Optional[datetime.date] = attr.ib()

    #  - What
    decision: Optional[StateEarlyDischargeDecision] = attr.ib()
    decision_raw_text: Optional[str] = attr.ib()
    decision_status: Optional[StateEarlyDischargeDecisionStatus] = attr.ib()
    decision_status_raw_text: Optional[str] = attr.ib()
    deciding_body_type: Optional[StateActingBodyType] = attr.ib()
    deciding_body_type_raw_text: Optional[str] = attr.ib()
    requesting_body_type: Optional[StateActingBodyType] = attr.ib()
    requesting_body_type_raw_text: Optional[str] = attr.ib()

    #   - Where
    county_code: str = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    early_discharge_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)

    # Should only be one of incarceration or supervision sentences on this.
    incarceration_sentence: Optional['StateIncarcerationSentence'] = attr.ib(default=None)
    supervision_sentence: Optional['StateIncarcerationSentence'] = attr.ib(default=None)


@attr.s(eq=False)
class StateSupervisionContact(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a person's contact with their supervising officer."""
    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Status
    status: Optional[StateSupervisionContactStatus] = attr.ib()
    status_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    contact_date: Optional[datetime.date] = attr.ib()

    #   - What
    contact_type: Optional[StateSupervisionContactType] = attr.ib()
    contact_type_raw_text: Optional[str] = attr.ib()

    contact_reason: Optional[StateSupervisionContactReason] = attr.ib()
    contact_reason_raw_text: Optional[str] = attr.ib()

    location: Optional[StateSupervisionContactLocation] = attr.ib()
    location_raw_text: Optional[str] = attr.ib()

    verified_employment: Optional[bool] = attr.ib()
    resulted_in_arrest: Optional[bool] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before we have written this entity to the
    # persistence layer
    supervision_contact_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    contacted_agent: Optional['StateAgent'] = attr.ib(default=None)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list)
