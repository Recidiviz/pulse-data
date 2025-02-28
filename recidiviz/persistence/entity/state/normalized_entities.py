# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Python representations of the tables in our state schema which represent data once it
has been run through the normalization portions of our pipelines.
"""
import abc
from datetime import date, datetime
from functools import cached_property
from typing import Any, Optional, Type

import attr

from recidiviz.common import attr_validators
from recidiviz.common.attr_utils import is_list
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
from recidiviz.common.date import (
    DateOrDateTime,
    DateRange,
    DurationMixin,
    NonNegativeDateRange,
    PotentiallyOpenDateTimeRange,
)
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
    HasExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
    UniqueConstraint,
)
from recidiviz.persistence.entity.generate_primary_key import generate_primary_key
from recidiviz.persistence.entity.state.entity_field_validators import appears_with
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import (
    LedgerEntityMixin,
    SequencedEntityMixin,
)


##### VALIDATORS #####
class EntityBackedgeValidator:
    """Attrs validator that can be used on fields that contain back edges, aka references
    to entities that are closer to the root in the entity tree.

    This validator is set up so that the back edge class is retrieved at runtime, when
    it is already loaded properly, vs import time where, due to the circular nature of
    the entity definitions, it is not yet available.
    """

    def allow_nulls(self) -> bool:
        """If True, allow null values in this field even after the entity graph is
        fully-formed. This should likely only be True for backedges on entities with
        multiple different possible parent types.
        """
        return False

    @abc.abstractmethod
    def get_backedge_type(self) -> Type:
        pass

    def __call__(
        self, instance: Any, attribute: attr.Attribute, value: Any | None
    ) -> None:
        if value is None:
            return

        expected_backedge_type = self.get_backedge_type()

        if is_list(attribute):
            if not isinstance(value, list):
                raise ValueError(
                    f"Found [{attribute.name}] set on class "
                    f"[{type(instance).__name__}] which is not a list."
                )

            for item in value:
                if not isinstance(value, expected_backedge_type):
                    raise ValueError(
                        f"Found [{attribute.name}] list set on class "
                        f"{type(instance).__name__} with incorrect item type "
                        f"[{type(item)}]. Expected type [{expected_backedge_type}]."
                    )
            return

        if not isinstance(value, expected_backedge_type):
            raise ValueError(
                f"Found [{attribute.name}] set on class {type(instance).__name__} with "
                f"incorrect type [{type(value)}]. Expected type "
                f"[{expected_backedge_type}]."
            )


class IsNormalizedPersonBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStatePerson


class IsNormalizedViolationBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSupervisionViolation


class IsNormalizedViolationResponseBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSupervisionViolationResponse


class IsNormalizedSentenceBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSentence


class IsNormalizedSentenceGroupBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSentenceGroup


class IsNormalizedSentenceInferredGroupBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSentenceInferredGroup


class IsNormalizedIncarcerationSentenceBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateIncarcerationSentence


class IsNormalizedSupervisionSentenceBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSupervisionSentence


class IsOptionalNormalizedIncarcerationSentenceBackedgeValidator(
    EntityBackedgeValidator
):
    def allow_nulls(self) -> bool:
        return True

    def get_backedge_type(self) -> Type:
        return NormalizedStateIncarcerationSentence


class IsOptionalNormalizedSupervisionSentenceBackedgeValidator(EntityBackedgeValidator):
    def allow_nulls(self) -> bool:
        return True

    def get_backedge_type(self) -> Type:
        return NormalizedStateSupervisionSentence


class IsNormalizedSupervisionPeriodBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateSupervisionPeriod


class IsNormalizedIncarcerationIncidentBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateIncarcerationIncident


class IsNormalizedStaffBackedgeValidator(EntityBackedgeValidator):
    def get_backedge_type(self) -> Type:
        return NormalizedStateStaff


##### END VALIDATORS #####


@attr.s(eq=False, kw_only=True)
class NormalizedStatePersonExternalId(NormalizedStateEntity, ExternalIdEntity):
    """Models an external id associated with a particular StatePerson."""

    # Primary key
    person_external_id_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
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
class NormalizedStatePersonAlias(NormalizedStateEntity, Entity):
    """Models an alias associated with a particular StatePerson."""

    # Attributes
    full_name: str = attr.ib(validator=attr_validators.is_str)
    alias_type: StatePersonAliasType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StatePersonAliasType)
    )
    alias_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_alias_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStatePersonRace(NormalizedStateEntity, EnumEntity):
    """Models a race associated with a particular StatePerson."""

    # Attributes
    race: StateRace = attr.ib(validator=attr.validators.instance_of(StateRace))
    race_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_race_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStatePersonEthnicity(NormalizedStateEntity, EnumEntity):
    """Models an ethnicity associated with a particular StatePerson."""

    # Attributes
    ethnicity: StateEthnicity = attr.ib(
        validator=attr.validators.instance_of(StateEthnicity)
    )
    ethnicity_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_ethnicity_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateAssessment(
    NormalizedStateEntity, HasExternalIdEntity, SequencedEntityMixin
):
    """Models a StateAssessment conducted about a particular StatePerson."""

    # Status
    # N/A - Always "COMPLETED", for now

    # Type
    assessment_class: StateAssessmentClass | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentClass)
    )
    assessment_class_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    assessment_type: StateAssessmentType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentType)
    )
    assessment_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    assessment_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    assessment_score: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    assessment_level: StateAssessmentLevel | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateAssessmentLevel)
    )
    assessment_level_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    assessment_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |conducting_agent| below
    conducting_staff_external_id: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("conducting_staff_external_id_type"),
            appears_with("conducting_staff_id"),
        ],
    )
    conducting_staff_external_id_type: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("conducting_staff_external_id"),
        ],
    )

    # Primary key
    assessment_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships

    # Only optional when hydrated in the parsing layer, before we have written this
    # entity to the persistence layer
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # A string representing an interval category based on assessment score
    assessment_score_bucket: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # StateStaff id foreign key for the conducting officer
    conducting_staff_id: int | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_int,
            appears_with("conducting_staff_external_id"),
        ],
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #


@attr.s(eq=False, kw_only=True)
class NormalizedStateChargeV2(NormalizedStateEntity, HasExternalIdEntity):
    """A formal allegation of an offense with information about the context for how that allegation was brought forth.
    `date_charged` can be null for charges that have statuses like “DROPPED”
    `offense_date` can be null because of erroneous data from states

    TODO(#26240): Replace NormalizedStateCharge with this entity
    """

    status: StateChargeV2Status = attr.ib(
        validator=attr.validators.instance_of(StateChargeV2Status)
    )
    status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    offense_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    date_charged: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    ncic_code: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # A code corresponding to actual sentencing terms within a jurisdiction
    statute: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    description: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    attempted: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    classification_type: Optional[StateChargeV2ClassificationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateChargeV2ClassificationType)
    )
    classification_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # E.g. 'A' for Class A, '1' for Level 1, etc
    classification_subtype: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    offense_type: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    is_violent: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_sex_offense: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_drug: bool | None = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    counts: int | None = attr.ib(default=None, validator=attr_validators.is_opt_int)
    charge_notes: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    is_controlling: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    charging_entity: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    judge_full_name: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    judge_external_id: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    judicial_district_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    charge_v2_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    sentences: list["NormalizedStateSentence"] = attr.ib(
        factory=list,
        validator=IsNormalizedSentenceBackedgeValidator(),
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # The original, state-provided NCIC (National Crime Information Center) code for
    # this offense.
    ncic_code_external: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # A high-level category associated with the state-provided NCIC code (e.g.
    # Kidnapping, Bribery, etc).
    ncic_category_external: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The human-readable description associated with the offense, as indicated by state
    # data or as inferred from the state-provided NCIC code.
    description_external: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Whether the charge is for a violent offense, as indicated by state data or as
    # inferred from the state-provided NCIC code.
    is_violent_external: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # Whether the charge is for a drug-related offense, as indicated by state data or as
    # inferred from the state-provided NCIC code.
    is_drug_external: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # Whether the charge is classified as sex offense, as indicated by state data or as
    # inferred by the state-provided NCIC code.
    is_sex_offense_external: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="charge_v2_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateSentenceStatusSnapshot(
    NormalizedStateEntity, LedgerEntityMixin, Entity
):
    """Represents a historical snapshot for when a given sentence had a given status."""

    # The start of the period of time over which the sentence status is valid
    status_update_datetime: datetime = attr.ib(
        validator=attr_validators.is_not_future_datetime
    )
    # The end of the period of time over which the sentence status is valid.
    # This will be None if the status is actively serving or terminated.
    status_end_datetime: datetime | None = attr.ib(
        validator=attr_validators.is_opt_not_future_datetime
    )
    # The status of a sentence
    status: StateSentenceStatus = attr.ib(
        validator=attr.validators.instance_of(StateSentenceStatus)
    )

    # The raw text value of the status of the sentence
    status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    sentence_status_snapshot_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    sentence: Optional["NormalizedStateSentence"] = attr.ib(
        default=None, validator=IsNormalizedSentenceBackedgeValidator()
    )

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        return self.status_update_datetime


@attr.s(eq=False, kw_only=True)
class NormalizedStateSentenceLength(NormalizedStateEntity, LedgerEntityMixin, Entity):
    """Represents a historical ledger of time attributes for a single sentence,
    including sentence length, accrued amount of days earned off, key dates, etc.

    The projected date fields for this entity can only be hydrated if the state provides
    sentence-specific estimates for individual sentences. Many states only provide projected
    dates for all sentences in a sentence group. If that is the case, hydrate the projected
    date fields on StateSentenceGroup.
    """

    # The start of the period of time over which the set of all sentence length attributes are valid
    length_update_datetime: datetime = attr.ib(
        validator=attr_validators.is_not_future_datetime
    )
    # The minimum duration of this sentence in days
    sentence_length_days_min: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # The maximum duration of this sentence in days
    sentence_length_days_max: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # Any good time (in days) the person has credited against this sentence due to good conduct,
    # a.k.a. time off for good behavior, if applicable.
    good_time_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # Any earned time (in days) the person has credited against this sentence due to participation
    # in programming designed to reduce the likelihood of re-offense, if applicable.
    earned_time_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    # The date on which a person is expected to become eligible for parole under the terms of this sentence
    parole_eligibility_date_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The date on which a person is projected to be released from incarceration to parole, if
    # they will be released to parole from the parent sentence.
    projected_parole_release_date_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The earliest date on which a person is projected to be released to liberty, if
    # they will be released to liberty from the parent sentence.
    projected_completion_date_min_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The latest date on which a person is projected to be released to liberty, if
    # they will be released to liberty from the parent sentence.
    projected_completion_date_max_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # Primary key
    sentence_length_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    sentence: Optional["NormalizedStateSentence"] = attr.ib(
        default=None, validator=IsNormalizedSentenceBackedgeValidator()
    )

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
class NormalizedStateSentenceGroupLength(
    NormalizedStateEntity, LedgerEntityMixin, Entity
):
    """Represents a historical ledger of attributes relating to a state designated group of sentences."""

    # The date when all sentence term attributes are updated
    group_update_datetime: datetime = attr.ib(
        validator=attr_validators.is_not_future_datetime
    )

    # The date on which a person is expected to become eligible for parole under the terms of this sentence
    parole_eligibility_date_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The date on which a person is projected to be released from incarceration to parole
    projected_parole_release_date_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The earliest date on which a person is projected to be released to liberty after having completed
    # all sentences in the term.
    projected_full_term_release_date_min_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    # The latest date on which a person is projected to be released to liberty after having completed
    # all sentences in the term.
    projected_full_term_release_date_max_external: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # Primary key
    # in the criminal justice system.
    sentence_group_length_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    sentence_group: Optional["NormalizedStateSentenceGroup"] = attr.ib(
        default=None, validator=IsNormalizedSentenceGroupBackedgeValidator()
    )

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
    def ledger_partition_columns(self) -> list[str]:
        return []

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        return self.group_update_datetime


@attr.s(eq=False, kw_only=True)
class NormalizedStateSentenceInferredGroup(NormalizedStateEntity, HasExternalIdEntity):
    """
    Represents a logical grouping of sentences that encompass an
    individual's interactions with a department of corrections.
    It begins with an individual's first sentence imposition and ends at liberty.
    This is a state agnostic term used by Recidiviz for a state
    specific administrative phenomena.

    While these groups are not state provided, we hydrate an external_id field
    that is a sorted concatenation of the external IDs of the sentences in the group.

    Inferred groups have sentences that have any ONE of the following ("or logic"):
      - sentences are a part of the same NormalizedStateSentenceGroup
      - sentences share an imposed_date
      - sentences have an overlapping span of an active SERVING status,
        meaning one sentence's first SERVING status is between another
        sentence's first SERVING status and terminating status

    Sentences can only be in a single inferred group.

    For example: if sentences A & B are in the same NormalizedStateSentenceGroup,
    and sentence C overlaps the SERVING status of sentence B, then A, B, and C
    are in the same inferred group.

    We build inferred groups from sentences because:
      - not all states necessarily have a StateSentenceGroup
      - all hydrated NormalizedStateSentenceGroup entities must have an
        associated NormalizedStateSentence entity
    """

    sentence_inferred_group_id: int = attr.ib(
        validator=attr_validators.is_int,
        # TODO(#32690) Update this when PK PK generation is consistent across
        # HasExternalId entities. This allows us to have a unique mandatory field for now
        default=attr.Factory(
            lambda s: generate_primary_key(s.external_id, StateCode(s.state_code)),
            takes_self=True,
        ),
    )
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    @property
    def sentence_external_ids(self) -> list[str]:
        """Returns the external ID for each sentence in this inferred group."""
        return self.external_id.split(self.external_id_delimiter())

    @classmethod
    def external_id_delimiter(cls) -> str:
        """This value separates sentence external IDs in this entity's external ID"""
        return "@#@"


@attr.s(eq=False, kw_only=True)
class NormalizedStateSentenceGroup(NormalizedStateEntity, HasExternalIdEntity):
    """
    Represents a logical grouping of sentences that encompass an
    individual's interactions with a department of corrections.
    It begins with an individual's first sentence imposition and ends at liberty.
    This is a state agnostic term used by Recidiviz for a state
    specific administrative phenomena.

    StateSentenceGroup entities must be associated with a sentence (StateSentence.sentence_external_id)
    to be hydrated.
    """

    # Unique internal identifier for a sentence group
    # Primary key
    sentence_group_id: int = attr.ib(validator=attr_validators.is_int)
    # A state provided sentence group is included in a single inferred sentence group
    sentence_inferred_group_id: int | None = attr.ib(
        validator=attr_validators.is_opt_int
    )
    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )
    sentence_group_lengths: list["NormalizedStateSentenceGroupLength"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSentenceGroupLength),
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateSentence(NormalizedStateEntity, HasExternalIdEntity):
    """Represents a formal judgement imposed by the court that details the form of time served
    in response to the set of charges for which someone was convicted.
    This table contains all the attributes we can observe about the sentence at the time of sentence imposition,
    all of which will remain static over the course of the sentence being served.
    """

    # Unique internal identifier for a sentence
    # Primary key
    sentence_id: int = attr.ib(validator=attr_validators.is_int)

    sentence_group_external_id: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # A sentence is included in a single inferred sentence group
    sentence_inferred_group_id: int | None = attr.ib(
        validator=attr_validators.is_opt_int
    )

    # The date this sentence was imposed, e.g. the date of actual sentencing,
    # but not necessarily the date the person started serving the sentence.
    # This value is only optional if:
    #   - this is a paritially hydrated entity (not merged yet)
    #   - it has sentencing_authority = StateSentencingAuthority.OTHER_STATE
    imposed_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The amount of any time already served (in days) at time of sentence imposition,
    # to possibly be credited against the overall sentence duration.
    initial_time_served_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # The type of sentence INCARCERATION, PROBATION, etc.
    sentence_type: StateSentenceType = attr.ib(
        validator=attr.validators.instance_of(StateSentenceType)
    )

    # The class of authority imposing this sentence: COUNTY, STATE, etc.
    # A value of COUNTY means a county court imposed this sentence.
    sentencing_authority: StateSentencingAuthority = attr.ib(
        validator=attr.validators.instance_of(StateSentencingAuthority)
    )
    sentencing_authority_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Raw text indicating whether a sentence is supervision/incarceration/etc
    sentence_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # True if this is sentence is for a life sentence
    is_life: bool | None = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    # True if this is sentence is for the death penalty
    is_capital_punishment: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # True if the person may be released to parole under the terms of this sentence
    # (only relevant to :INCARCERATION: sentence type)
    parole_possible: bool | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_bool,
    )

    # The code of the county under whose jurisdiction the sentence was imposed
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Identifier of the sentences to which this sentence is consecutive (external_id),
    # formatted as a string of comma-separated id’s. For instance, if sentence C has a
    # consecutive_sentence_id_array of [A, B], then both A and B must be completed before C can be served.
    # String must be parseable as a comma-separated list.
    parent_sentence_external_id_array: str | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )

    # A comma-separated list of conditions of this sentence which the person must follow to avoid a disciplinary
    # response. If this field is empty, there may still be applicable conditions that apply to someone's current term
    # of supervision/incarceration - either inherited from another ongoing sentence or the current supervision term.
    # (See conditions on StateSupervisionPeriod).
    conditions: str | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )

    # Additional metadata field with additional sentence attributes
    sentence_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    charges: list["NormalizedStateChargeV2"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateChargeV2),
    )
    sentence_status_snapshots: list["NormalizedStateSentenceStatusSnapshot"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSentenceStatusSnapshot),
    )
    sentence_lengths: list["NormalizedStateSentenceLength"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSentenceLength),
    )

    @cached_property
    def first_serving_status_to_terminating_status_dt_range(
        self,
    ) -> PotentiallyOpenDateTimeRange | None:
        if not self.sentence_status_snapshots:
            return None
        snapshots = sorted(
            self.sentence_status_snapshots, key=lambda s: s.partition_key
        )
        # Give no range if there is not a SERVING status
        try:
            first_serving_snapshot = [
                s for s in snapshots if s.status == StateSentenceStatus.SERVING
            ][0]
        except IndexError:
            return None

        final_snapshot = snapshots[-1]
        end_dt = (
            final_snapshot.status_update_datetime
            if final_snapshot.status.is_terminating_status
            else None
        )
        return PotentiallyOpenDateTimeRange(
            lower_bound_inclusive=first_serving_snapshot.status_update_datetime,
            upper_bound_exclusive=end_dt,
        )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="sentence_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            ),
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateEarlyDischarge(NormalizedStateEntity, HasExternalIdEntity):
    """Models a person's sentenced-level early discharge requests."""

    # Attributes
    #   - When
    request_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    decision_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #  - What
    decision: StateEarlyDischargeDecision | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateEarlyDischargeDecision)
    )
    decision_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    decision_status: StateEarlyDischargeDecisionStatus | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateEarlyDischargeDecisionStatus),
    )
    decision_status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    deciding_body_type: StateActingBodyType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateActingBodyType)
    )
    deciding_body_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    requesting_body_type: StateActingBodyType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateActingBodyType)
    )
    requesting_body_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Where
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key
    early_discharge_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    # Should only be one of incarceration or supervision sentences on this.
    incarceration_sentence: Optional["NormalizedStateIncarcerationSentence"] = attr.ib(
        default=None,
        validator=IsOptionalNormalizedIncarcerationSentenceBackedgeValidator(),
    )

    supervision_sentence: Optional["NormalizedStateSupervisionSentence"] = attr.ib(
        default=None,
        validator=IsOptionalNormalizedSupervisionSentenceBackedgeValidator(),
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="early_discharge_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateCharge(NormalizedStateEntity, HasExternalIdEntity):
    """Models a StateCharge against a particular StatePerson."""

    # Status
    status: StateChargeStatus = attr.ib(
        validator=attr.validators.instance_of(StateChargeStatus)
    )  # non-nullable
    status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    # N/A

    # Attributes
    #   - When
    offense_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    date_charged: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    ncic_code: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # A code corresponding to actual sentencing terms within a jurisdiction
    statute: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    description: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    attempted: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    classification_type: StateChargeClassificationType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateChargeClassificationType)
    )
    classification_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # E.g. 'A' for Class A, '1' for Level 1, etc
    classification_subtype: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    offense_type: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    is_violent: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_sex_offense: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_drug: bool | None = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    counts: int | None = attr.ib(default=None, validator=attr_validators.is_opt_int)
    charge_notes: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    is_controlling: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    charging_entity: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    judge_full_name: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    judge_external_id: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    judicial_district_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    charge_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    incarceration_sentences: list["NormalizedStateIncarcerationSentence"] = attr.ib(
        factory=list,
        validator=IsNormalizedIncarcerationSentenceBackedgeValidator(),
    )

    supervision_sentences: list["NormalizedStateSupervisionSentence"] = attr.ib(
        factory=list,
        validator=IsNormalizedSupervisionSentenceBackedgeValidator(),
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # The original, state-provided NCIC (National Crime Information Center) code for
    # this offense.
    ncic_code_external: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # A high-level category associated with the state-provided NCIC code (e.g.
    # Kidnapping, Bribery, etc).
    ncic_category_external: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The human-readable description associated with the offense, as indicated by state
    # data or as inferred from the state-provided NCIC code.
    description_external: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Whether the charge is for a violent offense, as indicated by state data or as
    # inferred from the state-provided NCIC code.
    is_violent_external: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # Whether the charge is for a drug-related offense, as indicated by state data or as
    # inferred from the state-provided NCIC code.
    is_drug_external: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # Whether the charge is classified as sex offense, as indicated by state data or as
    # inferred by the state-provided NCIC code.
    is_sex_offense_external: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="charge_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionSentence(NormalizedStateEntity, HasExternalIdEntity):
    """Models a sentence for a supervisory period associated with one or more Charges
    against a StatePerson."""

    # Status
    status: StateSentenceStatus = attr.ib(
        validator=attr.validators.instance_of(StateSentenceStatus)
    )  # non-nullable
    status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    supervision_type: StateSupervisionSentenceSupervisionType | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionSentenceSupervisionType),
    )
    supervision_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When

    # The date the person was sentenced
    date_imposed: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date on which a sentence effectively begins being served, including any pre-trial jail detention time if applicable.
    effective_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_completion_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date the person finished serving this sentence
    completion_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    is_life: bool | None = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    #   - Where
    # The county where this sentence was issued
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    min_length_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    max_length_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    sentence_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    conditions: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - Who

    # Primary key
    supervision_sentence_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    charges: list["NormalizedStateCharge"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateCharge),
    )
    early_discharges: list["NormalizedStateEarlyDischarge"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateEarlyDischarge),
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateIncarcerationSentence(NormalizedStateEntity, HasExternalIdEntity):
    """Models a sentence for prison/jail time associated with one or more Charges
    against a StatePerson."""

    # Status
    status: StateSentenceStatus = attr.ib(
        validator=attr.validators.instance_of(StateSentenceStatus)
    )  # non-nullable
    status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Type
    incarceration_type: StateIncarcerationType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationType)
    )
    incarceration_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When

    # The date the person was sentenced
    date_imposed: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date on which a sentence effectively begins being served, including any pre-trial jail detention time if applicable.
    effective_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_min_release_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_max_release_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    parole_eligibility_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # The date the person finished serving this sentence
    completion_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where this sentence was issued
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    # These will be None if is_life is true
    min_length_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    max_length_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    is_life: bool | None = attr.ib(default=None, validator=attr_validators.is_opt_bool)
    is_capital_punishment: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    parole_possible: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    initial_time_served_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    good_time_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    earned_time_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    sentence_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    conditions: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - Who
    # See |person| in entity relationships below.

    # Primary key
    incarceration_sentence_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    charges: list["NormalizedStateCharge"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateCharge),
    )
    early_discharges: list["NormalizedStateEarlyDischarge"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateEarlyDischarge),
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_sentence_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateIncarcerationPeriod(
    NormalizedStateEntity, HasExternalIdEntity, DurationMixin, SequencedEntityMixin
):
    """Models an uninterrupted period of time that a StatePerson is incarcerated at a
    single facility as a result of a particular sentence.
    """

    # Type
    incarceration_type: StateIncarcerationType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationType)
    )
    incarceration_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    admission_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    release_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where the facility is located
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    facility: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    housing_unit: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    housing_unit_type: StateIncarcerationPeriodHousingUnitType | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodHousingUnitType),
    )
    housing_unit_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    housing_unit_category: Optional[
        StateIncarcerationPeriodHousingUnitCategory
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodHousingUnitCategory),
    )
    housing_unit_category_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    #   - What
    admission_reason: StateIncarcerationPeriodAdmissionReason | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodAdmissionReason),
    )
    admission_reason_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    release_reason: StateIncarcerationPeriodReleaseReason | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodReleaseReason),
    )
    release_reason_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    custody_level: StateIncarcerationPeriodCustodyLevel | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationPeriodCustodyLevel),
    )
    custody_level_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSpecializedPurposeForIncarceration),
    )
    specialized_purpose_for_incarceration_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The type of government entity directly responsible for the person in this period
    # of incarceration. Not necessarily the decision making authority.
    custodial_authority: StateCustodialAuthority | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateCustodialAuthority)
    )
    custodial_authority_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key
    incarceration_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #
    purpose_for_incarceration_subtype: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    incarceration_admission_violation_type: StateSupervisionViolationType | None = (
        attr.ib(
            default=None,
            validator=attr_validators.is_opt(StateSupervisionViolationType),
        )
    )
    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @property
    def duration(self) -> NonNegativeDateRange:
        """Generates a DateRange for the days covered by the incarceration period. Since
        DateRange is never open, if the incarceration period is still active,
        then the exclusive upper bound of the range is set to tomorrow.
        """
        if not self.admission_date:
            raise ValueError(
                f"Expected start date for period {self.incarceration_period_id}, "
                "found None"
            )

        duration_unsafe = DateRange.from_maybe_open_range(
            start_date=self.admission_date, end_date=self.release_date
        )

        return NonNegativeDateRange(
            duration_unsafe.lower_bound_inclusive_date,
            duration_unsafe.upper_bound_exclusive_date,
        )

    @property
    def start_date_inclusive(self) -> date | None:
        return self.admission_date

    @property
    def end_date_exclusive(self) -> date | None:
        return self.release_date

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_period_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionCaseTypeEntry(NormalizedStateEntity, EnumEntity):

    # Attributes
    #   - What
    case_type: StateSupervisionCaseType = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionCaseType)
    )
    case_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    supervision_case_type_entry_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    supervision_period: Optional["NormalizedStateSupervisionPeriod"] = attr.ib(
        default=None, validator=IsNormalizedSupervisionPeriodBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionPeriod(
    NormalizedStateEntity, HasExternalIdEntity, DurationMixin, SequencedEntityMixin
):
    """Models a distinct period of time that a StatePerson is under supervision as a
    result of a particular sentence."""

    # Type
    supervision_type: StateSupervisionPeriodSupervisionType | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodSupervisionType),
    )
    supervision_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    start_date: date = attr.ib(validator=attr_validators.is_date)
    termination_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    # The county where this person is being supervised
    county_code: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    supervision_site: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    admission_reason: StateSupervisionPeriodAdmissionReason | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodAdmissionReason),
    )
    admission_reason_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    termination_reason: StateSupervisionPeriodTerminationReason | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionPeriodTerminationReason),
    )
    termination_reason_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    supervision_level: StateSupervisionLevel | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionLevel)
    )
    supervision_level_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    supervision_period_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # The type of government entity directly responsible for the person on this period
    # of supervision. Not necessarily the decision making authority.
    custodial_authority: StateCustodialAuthority | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateCustodialAuthority)
    )
    custodial_authority_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    conditions: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    #   - Who
    # See |person| in entity relationships below.
    supervising_officer_staff_external_id: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("supervising_officer_staff_external_id_type"),
            appears_with("supervising_officer_staff_id"),
        ],
    )
    supervising_officer_staff_external_id_type: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("supervising_officer_staff_external_id"),
        ],
    )

    # Primary key
    supervision_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    case_type_entries: list["NormalizedStateSupervisionCaseTypeEntry"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSupervisionCaseTypeEntry),
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # StateStaff id foreign key for the supervising officer
    supervising_officer_staff_id: int | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_int,
            appears_with("supervising_officer_staff_external_id"),
        ],
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @property
    def duration(self) -> NonNegativeDateRange:
        """Generates a DateRange for the days covered by the supervision period. Since
        DateRange is never open, if the supervision period is still active, then the
        exclusive upper bound of the range is set to tomorrow.
        """
        if not self.start_date:
            raise ValueError(
                f"Expected start date for period {self.supervision_period_id}, "
                "found None"
            )

        duration_unsafe = DateRange.from_maybe_open_range(
            start_date=self.start_date, end_date=self.termination_date
        )

        return NonNegativeDateRange(
            duration_unsafe.lower_bound_inclusive_date,
            duration_unsafe.upper_bound_exclusive_date,
        )

    @property
    def start_date_inclusive(self) -> date | None:
        return self.start_date

    @property
    def end_date_exclusive(self) -> date | None:
        return self.termination_date


@attr.s(eq=False, kw_only=True)
class NormalizedStateIncarcerationIncidentOutcome(
    NormalizedStateEntity, HasExternalIdEntity
):
    """Models the documented outcome in response to some StateIncarcerationIncident."""

    # Type
    outcome_type: StateIncarcerationIncidentOutcomeType | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationIncidentOutcomeType),
    )
    outcome_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    date_effective: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    projected_end_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    hearing_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    report_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    outcome_description: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    punishment_length_days: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    #   - Who
    # See |person| below

    # Primary key
    incarceration_incident_outcome_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    incarceration_incident: Optional["NormalizedStateIncarcerationIncident"] = attr.ib(
        default=None, validator=IsNormalizedIncarcerationIncidentBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_incident_outcome_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateIncarcerationIncident(NormalizedStateEntity, HasExternalIdEntity):
    """Models a documented incident for a StatePerson while incarcerated."""

    # Status
    # N/A

    # Type
    incident_type: StateIncarcerationIncidentType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationIncidentType)
    )
    incident_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    incident_severity: StateIncarcerationIncidentSeverity | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateIncarcerationIncidentSeverity),
    )
    incident_severity_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    incident_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - Where
    facility: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    location_within_facility: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    incident_details: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    incident_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    #  No fields

    # Primary key
    incarceration_incident_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    incarceration_incident_outcomes: list[
        "NormalizedStateIncarcerationIncidentOutcome"
    ] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(
            NormalizedStateIncarcerationIncidentOutcome
        ),
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="incarceration_incident_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionViolationTypeEntry(NormalizedStateEntity, EnumEntity):
    """Models a violation type associated with a particular
    StateSupervisionViolation."""

    # Attributes
    violation_type: StateSupervisionViolationType = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionViolationType)
    )
    violation_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    supervision_violation_type_entry_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    supervision_violation: Optional["NormalizedStateSupervisionViolation"] = attr.ib(
        default=None, validator=IsNormalizedViolationBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionViolatedConditionEntry(
    NormalizedStateEntity, EnumEntity
):
    """Models a condition applied to a supervision sentence, whose violation may be
    recorded in a StateSupervisionViolation.
    """

    # Attributes
    # An enum corresponding to the condition
    condition: StateSupervisionViolatedConditionType = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionViolatedConditionType),
    )

    # The most granular information from the state about the specific supervision condition that was violated
    condition_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    supervision_violated_condition_entry_id: int = attr.ib(
        validator=attr_validators.is_int
    )

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    supervision_violation: Optional["NormalizedStateSupervisionViolation"] = attr.ib(
        default=None, validator=IsNormalizedViolationBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionViolationResponseDecisionEntry(
    NormalizedStateEntity, EnumEntity
):
    """Models the type of decision resulting from a response to a
    StateSupervisionViolation."""

    # Attributes
    decision: StateSupervisionViolationResponseDecision = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionViolationResponseDecision)
    )
    decision_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    supervision_violation_response_decision_entry_id: int = attr.ib(
        validator=attr_validators.is_int
    )

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    supervision_violation_response: Optional[
        "NormalizedStateSupervisionViolationResponse"
    ] = attr.ib(
        default=None, validator=IsNormalizedViolationResponseBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionViolationResponse(
    NormalizedStateEntity, HasExternalIdEntity, SequencedEntityMixin
):
    """Models a response to a StateSupervisionViolation"""

    # Status
    # N/A

    # Type
    response_type: StateSupervisionViolationResponseType | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionViolationResponseType),
    )
    response_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    response_subtype: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    response_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    is_draft: bool | None = attr.ib(default=None, validator=attr_validators.is_opt_bool)

    violation_response_metadata: str | None = attr.ib(
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
    deciding_body_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    deciding_staff_external_id: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("deciding_staff_external_id_type"),
            appears_with("deciding_staff_id"),
        ],
    )
    deciding_staff_external_id_type: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("deciding_staff_external_id"),
        ],
    )

    # Primary key
    supervision_violation_response_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    supervision_violation: Optional["NormalizedStateSupervisionViolation"] = attr.ib(
        default=None, validator=IsNormalizedViolationBackedgeValidator()
    )

    supervision_violation_response_decisions: list[
        "NormalizedStateSupervisionViolationResponseDecisionEntry"
    ] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(
            NormalizedStateSupervisionViolationResponseDecisionEntry
        ),
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # StateStaff id foreign key for the deciding officer
    deciding_staff_id: int | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_int,
            appears_with("deciding_staff_external_id"),
        ],
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="supervision_violation_response_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionViolation(NormalizedStateEntity, HasExternalIdEntity):
    """
    Models a recorded instance where a StatePerson has violated one or more of the
    conditions of their StateSupervisionSentence.
    """

    # Status
    # N/A

    # Attributes
    #   - When
    violation_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    # These should correspond to |conditions| in StateSupervisionPeriod
    is_violent: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_sex_offense: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    violation_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key
    supervision_violation_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    supervision_violation_types: list[
        "NormalizedStateSupervisionViolationTypeEntry"
    ] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(
            NormalizedStateSupervisionViolationTypeEntry
        ),
    )
    supervision_violated_conditions: list[
        "NormalizedStateSupervisionViolatedConditionEntry"
    ] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(
            NormalizedStateSupervisionViolatedConditionEntry
        ),
    )
    supervision_violation_responses: list[
        "NormalizedStateSupervisionViolationResponse"
    ] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(
            NormalizedStateSupervisionViolationResponse
        ),
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="supervision_violation_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateProgramAssignment(
    NormalizedStateEntity, HasExternalIdEntity, SequencedEntityMixin
):
    """Models an person's assignment to a particular program."""

    # Status
    participation_status: StateProgramAssignmentParticipationStatus = attr.ib(
        validator=attr.validators.instance_of(StateProgramAssignmentParticipationStatus)
    )  # non-nullable
    participation_status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    referral_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    start_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    discharge_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    program_id: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    program_location_id: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    referral_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.
    referring_staff_external_id: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("referring_staff_external_id_type"),
            appears_with("referring_staff_id"),
        ],
    )
    referring_staff_external_id_type: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("referring_staff_external_id"),
        ],
    )
    # Primary key
    program_assignment_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # StateStaff id foreign key for the referring officer
    referring_staff_id: int | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_int,
            appears_with("referring_staff_external_id"),
        ],
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="program_assignment_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateSupervisionContact(NormalizedStateEntity, HasExternalIdEntity):
    """Models a person's contact with their supervising officer."""

    # Status
    status: StateSupervisionContactStatus | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactStatus)
    )
    status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    contact_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    contact_type: StateSupervisionContactType | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactType)
    )
    contact_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    supervision_contact_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - How
    contact_method: StateSupervisionContactMethod | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactMethod)
    )
    contact_method_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    contact_reason: StateSupervisionContactReason | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactReason)
    )
    contact_reason_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    location: StateSupervisionContactLocation | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateSupervisionContactLocation)
    )
    location_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    verified_employment: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    resulted_in_arrest: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    #   - Who
    # See |person| in entity relationships below.
    contacting_staff_external_id: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("contacting_staff_external_id_type"),
            appears_with("contacting_staff_id"),
        ],
    )
    contacting_staff_external_id_type: str | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("contacting_staff_external_id"),
        ],
    )

    # Primary key
    supervision_contact_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    # ~~~~~ FIELDS ADDED DURING NORMALIZATION ~~~~~ #

    # StateStaff id foreign key for the contacting officer
    contacting_staff_id: int | None = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_int,
            appears_with("contacting_staff_external_id"),
        ],
    )

    # ~~~ END FIELDS ADDED DURING NORMALIZATION ~~~ #

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="supervision_contact_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateEmploymentPeriod(NormalizedStateEntity, HasExternalIdEntity):
    """Models information about a person's employment status during a certain period of
    time.
    """

    # Status
    employment_status: StateEmploymentPeriodEmploymentStatus | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateEmploymentPeriodEmploymentStatus),
    )
    employment_status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    start_date: date = attr.ib(validator=attr_validators.is_date)
    end_date: date | None = attr.ib(default=None, validator=attr_validators.is_opt_date)
    last_verified_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    #   - What
    employer_name: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    employer_address: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    job_title: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    end_reason: StateEmploymentPeriodEndReason | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateEmploymentPeriodEndReason)
    )
    end_reason_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key
    employment_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="employment_period_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateDrugScreen(NormalizedStateEntity, HasExternalIdEntity):
    """The StateDrugScreen object represents information about a person's drug screen results for a given date."""

    # Status
    drug_screen_result: StateDrugScreenResult | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateDrugScreenResult),
    )
    drug_screen_result_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    drug_screen_date: date = attr.ib(validator=attr_validators.is_date)

    #   - What
    sample_type: StateDrugScreenSampleType | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateDrugScreenSampleType),
    )
    sample_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    drug_screen_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - Who
    # See |person| in entity relationships below.

    # Primary key
    drug_screen_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="state_drug_screen_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateTaskDeadline(NormalizedStateEntity, LedgerEntityMixin, Entity):
    """The StateTaskDeadline object represents a single task that should be performed as
    part of someone’s supervision or incarceration term, along with an associated date
    that task can be started and/or a deadline when that task must be completed.
    """

    # Attributes
    #   - When
    eligible_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    due_date: date | None = attr.ib(default=None, validator=attr_validators.is_opt_date)
    update_datetime: datetime = attr.ib(
        validator=attr_validators.is_not_future_datetime
    )

    #   - What
    task_type: StateTaskType = attr.ib(
        validator=attr.validators.instance_of(StateTaskType),
    )
    task_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    task_subtype: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    task_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    task_deadline_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )

    @classmethod
    def entity_tree_unique_constraints(cls) -> list[UniqueConstraint]:
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
    def ledger_partition_columns(self) -> list[str]:
        return ["task_type", "task_subtype", "task_metadata"]

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        """StateTaskDeadline ledger updates happen on update_datetime."""
        return self.update_datetime

    def __attrs_post_init__(self) -> None:
        """StateTaskDeadlines have an eligible date before a due date."""
        self.assert_datetime_less_than(self.eligible_date, self.due_date)


@attr.s(eq=False, kw_only=True)
class NormalizedStatePersonAddressPeriod(NormalizedStateEntity, EnumEntity):
    """Models an address associated with a particular StatePerson."""

    # Attributes
    address_line_1: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_line_2: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_city: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_zip: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_county: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_start_date: date = attr.ib(validator=attr_validators.is_date)

    address_end_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    address_is_verified: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    address_type: StatePersonAddressType = attr.ib(
        validator=attr.validators.instance_of(StatePersonAddressType)
    )

    address_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_metadata: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_address_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStatePersonHousingStatusPeriod(NormalizedStateEntity, EnumEntity):
    """Models a housing status period associated with a particular StatePerson."""

    # Attributes
    housing_status_start_date: date = attr.ib(validator=attr_validators.is_date)

    housing_status_end_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    housing_status_type: StatePersonHousingStatusType = attr.ib(
        validator=attr.validators.instance_of(StatePersonHousingStatusType),
    )

    housing_status_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_housing_status_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(
        default=None, validator=IsNormalizedPersonBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStatePerson(
    NormalizedStateEntity,
    HasMultipleExternalIdsEntity[NormalizedStatePersonExternalId],
    RootEntity,
):
    """Models a StatePerson moving through the criminal justice system."""

    # Attributes

    #   - Where
    current_address: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    full_name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    birthdate: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    gender: StateGender | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateGender)
    )
    gender_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # NOTE: This may change over time - we track these changes in history tables
    residency_status: StateResidencyStatus | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateResidencyStatus)
    )
    residency_status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    current_email_address: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    current_phone_number: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    external_ids: list["NormalizedStatePersonExternalId"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStatePersonExternalId),
    )
    aliases: list["NormalizedStatePersonAlias"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStatePersonAlias),
    )
    races: list["NormalizedStatePersonRace"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStatePersonRace),
    )
    ethnicities: list["NormalizedStatePersonEthnicity"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStatePersonEthnicity),
    )
    assessments: list["NormalizedStateAssessment"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateAssessment),
    )
    sentences: list["NormalizedStateSentence"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSentence),
    )
    incarceration_sentences: list["NormalizedStateIncarcerationSentence"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateIncarcerationSentence),
    )
    supervision_sentences: list["NormalizedStateSupervisionSentence"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSupervisionSentence),
    )
    incarceration_periods: list["NormalizedStateIncarcerationPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateIncarcerationPeriod),
    )
    supervision_periods: list["NormalizedStateSupervisionPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSupervisionPeriod),
    )
    program_assignments: list["NormalizedStateProgramAssignment"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateProgramAssignment),
    )
    incarceration_incidents: list["NormalizedStateIncarcerationIncident"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateIncarcerationIncident),
    )
    supervision_violations: list["NormalizedStateSupervisionViolation"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSupervisionViolation),
    )
    supervision_contacts: list["NormalizedStateSupervisionContact"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSupervisionContact),
    )
    task_deadlines: list["NormalizedStateTaskDeadline"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateTaskDeadline),
    )
    drug_screens: list["NormalizedStateDrugScreen"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateDrugScreen),
    )
    employment_periods: list["NormalizedStateEmploymentPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateEmploymentPeriod),
    )
    address_periods: list["NormalizedStatePersonAddressPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStatePersonAddressPeriod),
    )
    housing_status_periods: list["NormalizedStatePersonHousingStatusPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStatePersonHousingStatusPeriod),
    )
    sentence_groups: list["NormalizedStateSentenceGroup"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSentenceGroup),
    )
    sentence_inferred_groups: list["NormalizedStateSentenceInferredGroup"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateSentenceInferredGroup),
    )

    def get_external_ids(self) -> list["NormalizedStatePersonExternalId"]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "person"


@attr.s(eq=False, kw_only=True)
class NormalizedStateStaffExternalId(NormalizedStateEntity, ExternalIdEntity):
    """Models an external id associated with a particular StateStaff."""

    # Attributes
    #   - What
    id_type: str = attr.ib(validator=attr_validators.is_str)

    # Primary key
    staff_external_id_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    staff: Optional["NormalizedStateStaff"] = attr.ib(
        default=None, validator=IsNormalizedStaffBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_external_ids_unique_within_type_and_region",
                fields=["state_code", "id_type", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateStaffRolePeriod(NormalizedStateEntity, HasExternalIdEntity):
    """Represents information about a staff member’s role in the justice system during a
    particular period of time.
    """

    # Attributes
    #   - When
    start_date: date = attr.ib(validator=attr_validators.is_date)
    end_date: date | None = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - What
    role_type: StateStaffRoleType = attr.ib(
        validator=attr.validators.instance_of(StateStaffRoleType)
    )
    role_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    role_subtype: StateStaffRoleSubtype | None = attr.ib(
        default=None, validator=attr_validators.is_opt(StateStaffRoleSubtype)
    )
    role_subtype_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    staff_role_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    staff: Optional["NormalizedStateStaff"] = attr.ib(
        default=None, validator=IsNormalizedStaffBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_role_periods_unique_within_region",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateStaffSupervisorPeriod(NormalizedStateEntity, HasExternalIdEntity):
    """Represents information about a staff member’s direct supervisor during a
    particular period of time.
    """

    # Attributes
    #   - When
    start_date: date = attr.ib(validator=attr_validators.is_date)
    end_date: date | None = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - What
    supervisor_staff_external_id: str = attr.ib(validator=attr_validators.is_str)
    supervisor_staff_external_id_type: str = attr.ib(validator=attr_validators.is_str)

    # Primary key
    staff_supervisor_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    staff: Optional["NormalizedStateStaff"] = attr.ib(
        default=None, validator=IsNormalizedStaffBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_supervisor_periods_unique_within_region",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateStaffLocationPeriod(NormalizedStateEntity, HasExternalIdEntity):
    """Represents information about a period of time during which a staff member has
    a given assigned location.
    """

    # Attributes
    #   - When
    start_date: date = attr.ib(validator=attr_validators.is_date)
    end_date: date | None = attr.ib(default=None, validator=attr_validators.is_opt_date)

    #   - What
    location_external_id: str = attr.ib(validator=attr_validators.is_str)

    # Primary key
    staff_location_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    staff: Optional["NormalizedStateStaff"] = attr.ib(
        default=None, validator=IsNormalizedStaffBackedgeValidator()
    )

    @classmethod
    def global_unique_constraints(cls) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="staff_location_periods_unique_within_region",
                fields=["state_code", "external_id"],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStateStaffCaseloadTypePeriod(
    NormalizedStateEntity, HasExternalIdEntity
):
    """Represents information about a staff member’s caseload type over a period."""

    # The caseload type that the officer supervises
    caseload_type: StateStaffCaseloadType = attr.ib(
        validator=attr.validators.instance_of(StateStaffCaseloadType),
    )
    caseload_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # Attributes
    #   - When
    # The beginning of the period where this officer had this type of specialized caseload
    start_date: date = attr.ib(validator=attr_validators.is_date)

    # The end of the period where this officer had this type of specialized caseload
    end_date: date | None = attr.ib(default=None, validator=attr_validators.is_opt_date)
    # Primary key
    staff_caseload_type_period_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    staff: Optional["NormalizedStateStaff"] = attr.ib(
        default=None, validator=IsNormalizedStaffBackedgeValidator()
    )


@attr.s(eq=False, kw_only=True)
class NormalizedStateStaff(
    NormalizedStateEntity,
    HasMultipleExternalIdsEntity[NormalizedStateStaffExternalId],
    RootEntity,
):
    """Models a staff member working within a justice system."""

    # Attributes
    #   - What
    full_name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    # TODO(#29072): Add is_opt_valid_email validator once all states have valid emails
    email: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Primary key
    staff_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    external_ids: list["NormalizedStateStaffExternalId"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateStaffExternalId),
    )
    role_periods: list["NormalizedStateStaffRolePeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateStaffRolePeriod),
    )
    supervisor_periods: list["NormalizedStateStaffSupervisorPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateStaffSupervisorPeriod),
    )
    location_periods: list["NormalizedStateStaffLocationPeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateStaffLocationPeriod),
    )
    caseload_type_periods: list["NormalizedStateStaffCaseloadTypePeriod"] = attr.ib(
        factory=list,
        validator=attr_validators.is_list_of(NormalizedStateStaffCaseloadTypePeriod),
    )

    def get_external_ids(self) -> list[NormalizedStateStaffExternalId]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "staff"
