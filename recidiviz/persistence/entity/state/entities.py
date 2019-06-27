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

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate.
This allows these persistence layer objects additional flexibility that the SQL
Alchemy ORM objects can't provide.
"""

from typing import Optional, List

import datetime
import attr

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
)

from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
)
from recidiviz.common.constants.state.state_parole_decision import \
    StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import (
    StateSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionLevel,
)


from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import (
        StateSupervisionViolationResponseType,
        StateSupervisionViolationResponseDecision,
        StateSupervisionViolationResponseRevocationType,
        StateSupervisionViolationResponseDecidingBodyType,
    )

from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity


# **** Entity ordering template *****:

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

@attr.s(cmp=False)
class StatePersonExternalId(Entity, BuildableAttr, DefaultableAttr):
    """Models an external id associated with a particular StatePerson."""
    external_id: str = attr.ib()

    #   - Where
    # State providing the external id
    state_code: str = attr.ib()  # non-nullable

    #   - What
    # TODO(1905): Not optional in schema
    id_type: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    person_external_id_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(cmp=False)
class StatePersonAlias(Entity, BuildableAttr, DefaultableAttr):
    """Models an alias associated with a particular StatePerson."""
    # Attributes
    state_code: str = attr.ib()  # non-nullable
    # TODO(1905): Remove defaults for string fields
    full_name: Optional[str] = attr.ib(default=None)
    surname: Optional[str] = attr.ib(default=None)
    given_names: Optional[str] = attr.ib(default=None)
    middle_names: Optional[str] = attr.ib(default=None)
    name_suffix: Optional[str] = attr.ib(default=None)

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    person_alias_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(cmp=False)
class StatePersonRace(Entity, BuildableAttr, DefaultableAttr):
    """Models a race associated with a particular StatePerson."""
    # Attributes
    state_code: str = attr.ib()  # non-nullable
    race: Optional[Race] = attr.ib()
    race_raw_text: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    person_race_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(cmp=False)
class StatePersonEthnicity(Entity, BuildableAttr, DefaultableAttr):
    """Models an ethnicity associated with a particular StatePerson."""
    # Attributes
    state_code: str = attr.ib()  # non-nullable
    ethnicity: Optional[Ethnicity] = attr.ib()
    ethnicity_raw_text: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    person_ethnicity_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)


@attr.s(cmp=False)
class StatePerson(Entity, BuildableAttr, DefaultableAttr):
    """Models a StatePerson moving through the criminal justice system."""
    # Attributes

    #   - Where
    current_address: Optional[str] = attr.ib(default=None)

    #   - What
    full_name: Optional[str] = attr.ib(default=None)

    birthdate: Optional[datetime.date] = attr.ib(default=None)
    birthdate_inferred_from_age: Optional[bool] = attr.ib(default=None)

    gender: Optional[Gender] = attr.ib(default=None)
    gender_raw_text: Optional[str] = attr.ib(default=None)

    # NOTE: This may change over time - we track these changes in history tables
    residency_status: Optional[ResidencyStatus] = attr.ib(default=None)

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    person_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    external_ids: List['StatePersonExternalId'] = attr.ib(factory=list)
    aliases: List['StatePersonAlias'] = attr.ib(factory=list)
    races: List['StatePersonRace'] = attr.ib(factory=list)
    ethnicities: List['StatePersonEthnicity'] = attr.ib(factory=list)
    assessments: List['StateAssessment'] = attr.ib(factory=list)
    sentence_groups: List['StateSentenceGroup'] = attr.ib(factory=list)

    # NOTE: Eventually we might have a relationship to objects holding
    # pre-sentence information so we can track encounters with the justice
    # system that don't result in sentences.


@attr.s(cmp=False)
class StateBond(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateBond associated with a particular StateCharge."""
    # Status
    status: BondStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    bond_type: Optional[BondType] = attr.ib()
    bond_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    date_paid: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    county_code: Optional[str] = attr.ib()

    #   - What
    amount_dollars: Optional[int] = attr.ib()

    #   - Who
    bond_agent: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    bond_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateCourtCase(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCourtCase associated with some set of StateCharges"""
    # Status
    status: StateCourtCaseStatus = attr.ib()
    status_raw_text: Optional[str] = attr.ib()

    # Type
    court_type: Optional[StateCourtType] = attr.ib()
    court_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    date_convicted: Optional[datetime.date] = attr.ib()
    next_court_date: Optional[datetime.date] = attr.ib()

    #   - Where
    # Location of the court itself
    state_code: str = attr.ib()  # non-nullable
    # County where the court case took place
    county_code: Optional[str] = attr.ib()

    #   - What
    court_fee_dollars: Optional[int] = attr.ib()

    #   - Who
    # See |judge| below

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    court_case_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list)
    judge: Optional['StateAgent'] = attr.ib(default=None)


@attr.s(cmp=False)
class StateCharge(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateCharge against a particular StatePerson."""
    # Status
    status: ChargeStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    # N/A

    # Attributes
    #   - When
    offense_date: Optional[datetime.date] = attr.ib()
    date_charged: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    county_code: str = attr.ib()

    #   - What
    # A code corresponding to actual sentencing terms that
    statute: Optional[str] = attr.ib()
    description: Optional[str] = attr.ib()
    attempted: Optional[bool] = attr.ib()
    classification_type: Optional[StateChargeClassificationType] = attr.ib()
    classification_type_raw_text: Optional[str] = attr.ib()
    # E.g. 'A' for Class A, '1' for Level 1, etc
    classification_subtype: Optional[str] = attr.ib()
    counts: Optional[int] = attr.ib()
    charge_notes: Optional[str] = attr.ib()

    #   - Who
    charging_entity: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    charge_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    court_case: Optional['StateCourtCase'] = attr.ib(default=None)
    bond: Optional['StateBond'] = attr.ib(default=None)

    incarceration_sentences: List['StateIncarcerationSentence'] = \
        attr.ib(factory=list)
    supervision_sentences: List['StateSupervisionSentence'] = \
        attr.ib(factory=list)
    fines: List['StateFine'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateAssessment(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a StateAssessment conducted about a particular StatePerson."""
    # Status
    # N/A - Always "COMPLETED", for now

    # Type
    assessment_class: Optional[StateAssessmentClass] = attr.ib()
    assessment_class_raw_text: Optional[str] = attr.ib()
    assessment_type: Optional[StateAssessmentType] = attr.ib()
    assessment_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    assessment_date: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable

    #   - What
    assessment_score: Optional[int] = attr.ib()
    assessment_level: Optional[StateAssessmentLevel] = attr.ib()
    assessment_level_raw_text: Optional[str] = attr.ib()
    assessment_metadata: Optional[str] = attr.ib()

    #   - Who
    # See |conducting_agent| below

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    assessment_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships

    # Only optional when hydrated in the data converter, before we have written
    # this entity to the persistence layer
    person: Optional['StatePerson'] = attr.ib(default=None)
    incarceration_period: Optional['StateIncarcerationPeriod'] = \
        attr.ib(default=None)
    supervision_period: Optional['StateSupervisionPeriod'] = \
        attr.ib(default=None)
    conducting_agent: Optional['StateAgent'] = attr.ib(default=None)


@attr.s(cmp=False)
class StateSentenceGroup(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """
    Models a group of related sentences, which may be served consecutively or
    concurrently.
    """
    # Status
    # TODO(1698): Look at Measures for Justice doc for methodology on how to
    #  calculate an aggregate sentence status from multiple sentence statuses.
    # This will be a composite of all the linked individual statuses
    status: StateSentenceStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    # N/A

    # Attributes
    #   - When
    date_imposed: Optional[datetime.date] = attr.ib()
    # TODO(1698): Consider including rollup projected completion dates?

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib()

    #   - What
    # See |supervision_sentences|, |incarceration_sentences|, and |fines| in
    # entity relationships below for more of the What.
    # Total length periods, either rolled up from individual sentences
    # or directly reported from the ingested source data
    min_length_days: Optional[int] = attr.ib()
    max_length_days: Optional[int] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    sentence_group_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_sentences: List['StateSupervisionSentence'] = attr.ib(
        factory=list)
    incarceration_sentences: List['StateIncarcerationSentence'] = \
        attr.ib(factory=list)
    fines: List['StateFine'] = attr.ib(factory=list)
    # TODO(1698): Add information about the time relationship between individual
    #  sentences (i.e. consecutive vs concurrent).


@attr.s(cmp=False)
class StateSupervisionSentence(ExternalIdEntity,
                               BuildableAttr,
                               DefaultableAttr):
    """
    Models a sentence for a supervisory period associated with one or more
    Charges against a StatePerson.
    """
    # Status
    status: StateSentenceStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    supervision_type: Optional[StateSupervisionType] = attr.ib()
    supervision_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    projected_completion_date: Optional[datetime.date] = attr.ib()
    completion_date: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib()

    #   - What
    min_length_days: Optional[int] = attr.ib()
    max_length_days: Optional[int] = attr.ib()

    #   - Who

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    supervision_sentence_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    sentence_group: Optional['StateSentenceGroup'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list)

    # NOTE: A person might have an incarceration period associated with a
    # supervision sentence if they violate the terms of the sentence and are
    # sent back to prison.
    incarceration_periods: List['StateIncarcerationPeriod'] = attr.ib(
        factory=list)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateIncarcerationSentence(ExternalIdEntity,
                                 BuildableAttr,
                                 DefaultableAttr):
    """
    Models a sentence for prison/jail time associated with one or more Charges
    against a StatePerson.
    """
    # Status
    status: StateSentenceStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    incarceration_type: Optional[StateIncarcerationType] = attr.ib()
    incarceration_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    date_imposed: Optional[datetime.date] = attr.ib()
    projected_min_release_date: Optional[datetime.date] = attr.ib()
    projected_max_release_date: Optional[datetime.date] = attr.ib()
    parole_eligibility_date: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    # The county where this sentence was issued
    county_code: Optional[str] = attr.ib()

    #   - What
    # These will be None if is_life is true
    min_length_days: Optional[int] = attr.ib()
    max_length_days: Optional[int] = attr.ib()

    is_life: Optional[bool] = attr.ib()

    parole_possible: Optional[bool] = attr.ib()
    initial_time_served_days: Optional[int] = attr.ib()
    good_time_days: Optional[int] = attr.ib()
    earned_time_days: Optional[int] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    incarceration_sentence_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    sentence_group: Optional['StateSentenceGroup'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list)

    incarceration_periods: List['StateIncarcerationPeriod'] = attr.ib(
        factory=list)
    supervision_periods: List['StateSupervisionPeriod'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateFine(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """
    Models a fine that a StatePerson is sentenced to pay in association with a
    StateCharge.
    """
    # Status
    status: StateFineStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    # N/A

    # Attributes
    #   - When
    date_paid: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    # The county where this fine was issued
    county_code: Optional[str] = attr.ib()

    #   - What
    fine_dollars: Optional[int] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    fine_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    sentence_group: Optional['StateSentenceGroup'] = attr.ib(default=None)
    charges: List['StateCharge'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateIncarcerationPeriod(ExternalIdEntity,
                               BuildableAttr,
                               DefaultableAttr):
    """
    Models an uninterrupted period of time that a StatePerson is incarcerated at
    a single facility as a result of a particular sentence.
    """

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
    state_code: str = attr.ib()  # non-nullable
    # The county where the facility is located
    county_code: Optional[str] = attr.ib()

    facility: Optional[str] = attr.ib()
    housing_unit: Optional[str] = attr.ib()

    #   - What
    facility_security_level: \
        Optional[StateIncarcerationFacilitySecurityLevel] = attr.ib()
    facility_security_level_raw_text: Optional[str] = attr.ib()

    admission_reason: \
        Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib()
    admission_reason_raw_text: Optional[str] = attr.ib()

    projected_release_reason: \
        Optional[StateIncarcerationPeriodReleaseReason] = attr.ib()
    projected_release_reason_raw_text: Optional[str] = attr.ib()

    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = \
        attr.ib()
    release_reason_raw_text: Optional[str] = attr.ib()

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    incarceration_period_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)

    # NOTE: An incarceration period might count towards multiple sentences
    incarceration_sentences: List['StateIncarcerationSentence'] = \
        attr.ib(factory=list)
    supervision_sentences: List['StateSupervisionSentence'] = \
        attr.ib(factory=list)

    incarceration_incidents: List['StateIncarcerationIncident'] = \
        attr.ib(factory=list)
    parole_decisions: List['StateParoleDecision'] = attr.ib(factory=list)
    assessments: List['StateAssessment'] = attr.ib(factory=list)

    # When the admission reason is PROBATION_REVOCATION or PAROLE_REVOCATION,
    # this is the object with info about the violation/hearing that resulted in
    # the revocation
    source_supervision_violation_response: \
        Optional['StateSupervisionViolationResponse'] = attr.ib(default=None)


@attr.s(cmp=False)
class StateSupervisionPeriod(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """
    Models a distinct period of time that a StatePerson is under supervision as
    a result of a particular sentence.
    """
    # Status
    status: StateSupervisionPeriodStatus = attr.ib()  # non-nullable
    status_raw_text: Optional[str] = attr.ib()

    # Type
    supervision_type: Optional[StateSupervisionType] = attr.ib()
    supervision_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    start_date: Optional[datetime.date] = attr.ib()
    termination_date: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    # The county where this person is being supervised
    county_code: Optional[str] = attr.ib()

    #   - What
    admission_reason: \
        Optional[StateSupervisionPeriodAdmissionReason] = attr.ib()
    admission_reason_raw_text: Optional[str] = attr.ib()

    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = \
        attr.ib()
    termination_reason_raw_text: Optional[str] = attr.ib()

    supervision_level: Optional[StateSupervisionLevel] = attr.ib()
    supervision_level_raw_text: Optional[str] = attr.ib()

    conditions: Optional[str] = attr.ib(default=None)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    supervision_period_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)

    # NOTE: A supervision period might count towards multiple sentences
    incarceration_sentences: List['StateIncarcerationSentence'] = \
        attr.ib(factory=list)
    supervision_sentences: List['StateSupervisionSentence'] = \
        attr.ib(factory=list)

    supervision_violations: List['StateSupervisionViolation'] = attr.ib(
        factory=list)
    assessments: List['StateAssessment'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateIncarcerationIncident(ExternalIdEntity,
                                 BuildableAttr,
                                 DefaultableAttr):
    """Models a documented incident for a StatePerson while incarcerated."""
    # Status
    # N/A

    # Type
    incident_type: Optional[StateIncarcerationIncidentType] = attr.ib()
    incident_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    incident_date: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    facility: Optional[str] = attr.ib()
    location_within_facility: Optional[str] = attr.ib()

    #   - What
    incident_details: Optional[str] = attr.ib()

    #   - Who
    # See |responding_officer| below

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    incarceration_incident_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    responding_officer: Optional['StateAgent'] = attr.ib(default=None)
    incarceration_period: Optional['StateIncarcerationPeriod'] = \
        attr.ib(default=None)

    incarceration_incident_outcomes: List['StateIncarcerationIncidentOutcome'] \
        = attr.ib(factory=list)


@attr.s(cmp=False)
class StateIncarcerationIncidentOutcome(ExternalIdEntity,
                                        BuildableAttr,
                                        DefaultableAttr):
    # Type
    outcome_type: Optional[StateIncarcerationIncidentOutcomeType] = attr.ib()
    outcome_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    date_effective: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable

    #   - What
    outcome_description: Optional[str] = attr.ib()
    punishment_length_days: Optional[int] = attr.ib()

    #   - Who
    # See |person| below

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    incarceration_incident_outcome_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    incarceration_incident: Optional['StateIncarcerationIncident'] = \
        attr.ib(default=None)


@attr.s(cmp=False)
class StateParoleDecision(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models a Parole Decision for a StatePerson while under Incarceration."""
    # Status
    # TODO(1697): Remove this redundant field (can infer from decision_outcome)
    received_parole: Optional[bool] = attr.ib()

    # Type

    # Attributes
    #   - When
    decision_date: Optional[datetime.date] = attr.ib()
    corrective_action_deadline: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable
    # The county where the decision was made, if different from the county where
    # this person is incarcerated.
    county_code: Optional[str] = attr.ib()

    #   - What
    decision_outcome: Optional[StateParoleDecisionOutcome] = attr.ib()
    decision_outcome_raw_text: Optional[str] = attr.ib()
    decision_reasoning: Optional[str] = attr.ib()
    corrective_action: Optional[str] = attr.ib()

    #   - Who
    # See |decision_agents| below

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    parole_decision_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    incarceration_period: Optional['StateIncarcerationPeriod'] = \
        attr.ib(default=None)
    decision_agents: List['StateAgent'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateSupervisionViolation(ExternalIdEntity,
                                BuildableAttr,
                                DefaultableAttr):
    """
    Models a recorded instance where a StatePerson has violated one or more of
    the conditions of their StateSupervisionSentence.
    """
    # Status
    # N/A

    # Type
    violation_type: Optional[StateSupervisionViolationType] = attr.ib()
    violation_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    violation_date: Optional[datetime.date] = attr.ib()

    #   - Where
    # State that recorded this violation, not necessarily where the violation
    # took place
    state_code: str = attr.ib()  # non-nullable

    #   - What
    # These should correspond to |conditions| in StateSupervisionPeriod
    is_violent: Optional[bool] = attr.ib()

    violated_conditions: Optional[str] = attr.ib(default=None)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    supervision_violation_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_period: Optional['StateSupervisionPeriod'] = \
        attr.ib(default=None)
    supervision_violation_responses: \
        List['StateSupervisionViolationResponse'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateSupervisionViolationResponse(ExternalIdEntity,
                                        BuildableAttr,
                                        DefaultableAttr):
    """Models a response to a StateSupervisionViolation"""
    # Status
    # N/A

    # Type
    response_type: Optional[StateSupervisionViolationResponseType] = attr.ib()
    response_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - When
    response_date: Optional[datetime.date] = attr.ib()

    #   - Where
    state_code: str = attr.ib()  # non-nullable

    #   - What
    decision: Optional[StateSupervisionViolationResponseDecision] = attr.ib()
    decision_raw_text: Optional[str] = attr.ib()

    # Only nonnull if decision is REVOCATION
    revocation_type: \
        Optional[StateSupervisionViolationResponseRevocationType] = attr.ib()
    revocation_type_raw_text: Optional[str] = attr.ib()

    #   - Who
    # See SupervisionViolationResponders below
    deciding_body_type: \
        Optional[StateSupervisionViolationResponseDecidingBodyType] = attr.ib()
    deciding_body_type_raw_text: Optional[str] = attr.ib()
    # See also |deciding_agents| below

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    supervision_violation_response_id: Optional[int] = attr.ib(default=None)

    # Cross-entity relationships
    person: Optional['StatePerson'] = attr.ib(default=None)
    supervision_violation: Optional['StateSupervisionViolation'] = \
        attr.ib(default=None)
    decision_agents: List['StateAgent'] = attr.ib(factory=list)


@attr.s(cmp=False)
class StateAgent(ExternalIdEntity, BuildableAttr, DefaultableAttr):
    """Models an agent working within a justice system."""
    # Type
    agent_type: StateAgentType = attr.ib()
    agent_type_raw_text: Optional[str] = attr.ib()

    # Attributes
    #   - Where
    state_code: str = attr.ib()  # non-nullable

    #   - What
    full_name: Optional[str] = attr.ib()

    # Primary key - Only optional when hydrated in the data converter, before
    # we have written this entity to the persistence layer
    agent_id: Optional[int] = attr.ib(default=None)
