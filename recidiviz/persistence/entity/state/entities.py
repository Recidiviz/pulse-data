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

from recidiviz.common import attr_utils, attr_validators
from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.reasonable_dates import (
    BIRTHDATE_REASONABLE_LOWER_BOUND,
    MAX_DATE_FIELD_REASONABLE_UPPER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
    STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
    STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND,
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
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
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
    StateSupervisionViolationSeverity,
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseSeverity,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import (
    DateOrDateTime,
    DateRange,
    DurationMixin,
    PotentiallyOpenDateRange,
)
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
from recidiviz.persistence.entity.state.state_entity_mixins import (
    LedgerEntityMixin,
    StateEntityMixin,
)
from recidiviz.persistence.entity.state.state_entity_utils import (
    PARENT_SENTENCE_EXTERNAL_ID_SEPARATOR,
)

# **** Entity ordering template *****:

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
class StatePersonAddressPeriod(
    StateEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """
    A single StatePersonAddressPeriod entity represents a person's physical, mailing,
    or other address for a defined period of time.

    All StatePersonAddressPeriod entities for a given person can provide a historical look
    at their address history, while the most recent entity can provide a current known address.

    A person's StatePersonAddressPeriod entities' uniqueness are determined by the address
    characteristics, address start and end dates, and address type.
    """

    # Attributes
    address_line_1: str = attr.ib(validator=attr_validators.is_non_empty_str)

    address_line_2: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
        # TODO(#37668) Remove converter when parsers can return None
        converter=attr_utils.convert_empty_string_to_none,
    )

    address_city: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # TODO(#5508) Convert this to enum
    address_state: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_country: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_zip: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_county: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    address_start_date: datetime.date = attr.ib(
        validator=attr_validators.is_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
    )

    address_end_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
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

    @property
    def date_range(self) -> PotentiallyOpenDateRange:
        return PotentiallyOpenDateRange(self.address_start_date, self.address_end_date)

    full_address: str = attr.ib(validator=attr_validators.is_str)

    @full_address.default
    def _full_address(self) -> str:
        """
        Returns a full address like so:
            Required Line 1
            Optional Line 2
            Optional City, Optional State Optional ZIP
            Optional Country
        """
        lines = [self.address_line_1]
        if self.address_line_2:
            lines.append(self.address_line_2)

        city_state_zip = ""
        if self.address_city:
            city_state_zip += self.address_city
        if self.address_state:
            city_state_zip += (", " if self.address_city else "") + self.address_state
        if self.address_zip:
            city_state_zip += (
                f" {self.address_zip}"
                if self.address_city or self.address_state
                else self.address_zip
            )

        if city_state_zip.strip():
            lines.append(city_state_zip.strip())

        if self.address_country:
            lines.append(self.address_country)

        return "\n".join(lines)

    def __attrs_post_init__(self) -> None:
        if self._full_address() != self.full_address:
            raise ValueError(
                "Cannot construct a StatePersonAddressPeriod with a full_address that "
                "differs from the derived full_address."
            )


@attr.s(eq=False, kw_only=True)
class StatePersonHousingStatusPeriod(
    StateEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """
    The StatePersonHousingStatusPeriod object represents
    information about a person’s housing status during a particular
    period of time. This object can be used to identify when a person
    is currently, or has been previously, unhoused or living
    in temporary housing
    """

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
class StatePersonExternalId(
    StateEntityMixin, ExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    Each StatePersonExternalId holds a single external id provided by the source data system being
    ingested. An external id is a unique identifier for an individual, unique within the scope of
    the source data system. We include information denoting the source of the id to make this into
    a globally unique identifier.
    """

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_external_id_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    is_current_display_id_for_type: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    is_stable_id_for_type: bool | None = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    id_active_from_datetime: datetime.datetime | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_datetime(
            STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
        ),
    )
    id_active_to_datetime: datetime.datetime | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_datetime(
            STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
        ),
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        self.assert_datetime_less_than_or_equal(
            self.id_active_from_datetime,
            self.id_active_to_datetime,
            before_description="id active from datetime",
            after_description="id active to datetime",
        )

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
class StatePersonAlias(StateEntityMixin, Entity, BuildableAttr, DefaultableAttr):
    """
    Each StatePersonAlias holds the naming information for an alias for a particular
    person. Because a given name is an alias of sorts, we copy over the name fields
    provided on the StatePerson object into a child StatePersonAlias object. An alias
    is structured similarly to a name, with various different fields, and not a
    raw string -- systems storing aliases are raw strings should provide those in
    the full_name field below.
    """

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
class StatePersonRace(StateEntityMixin, EnumEntity, BuildableAttr, DefaultableAttr):
    """
    Each StatePersonRace holds a single reported race for a single person. A
    StatePerson may have multiple StatePersonRace objects because they may be
    multi-racial, or because different data sources may report different races.
    """

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
class StatePersonEthnicity(
    StateEntityMixin, EnumEntity, BuildableAttr, DefaultableAttr
):
    """
    Each StatePersonEthnicity holds a single reported ethnicity for a single person.
    A StatePerson may have multiple StatePersonEthnicity objects, because they may be
    multi-ethnic, or because different data sources may report different ethnicities.
    """

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
    StateEntityMixin,
    BuildableAttr,
    DefaultableAttr,
):
    """
    Each StatePerson holds details about the individual, as well as lists of several
    child entities. Some of these child entities are extensions of individual details,
    e.g. Race is its own entity as opposed to a single field, to allow for the
    inclusion/tracking of multiple such entities or sources of such information.
    """

    # Attributes

    #   - Where
    # TODO(#42457): Deprecate this field in favor of structured address data stored in
    #  StatePersonAddressPeriod.
    current_address: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    birthdate: datetime.date | None = attr.ib(
        default=None,
        # TODO(#40483): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=BIRTHDATE_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#36562): Stop hydrating CO ingest data
                    StateCode.US_CO,
                    # TODO(#40486): Fix bad dates so all non-null dates fall within the bounds (1700-01-01, <current date>).
                    #  - Found dates as high as 2068-12-29.
                    StateCode.US_MA,
                    # TODO(#40487): Fix bad dates so all non-null dates fall within the bounds (1700-01-01, <current date>).
                    #  - Found dates as high as 9999-01-01.
                    StateCode.US_ME,
                    # TODO(#40488): Fix bad dates so all non-null dates fall within the bounds (1700-01-01, <current date>).
                    #  - Found dates as low as 0001-01-01.
                    StateCode.US_NC,
                    # TODO(#40489): Fix bad dates so all non-null dates fall within the bounds (1700-01-01, <current date>).
                    #  - Found dates as low as 0973-07-14.
                    #  - Found dates as high as 6196-10-12.
                    StateCode.US_UT,
                },
            ),
        ),
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
    staff_relationship_periods: List["StatePersonStaffRelationshipPeriod"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    def get_external_ids(self) -> List["StatePersonExternalId"]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "person"


@attr.s(eq=False, kw_only=True)
class StateCharge(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateCharge object holds information on a single charge that a person has been accused of.
    A single StateCharge can reference multiple Incarceration/Supervision Sentences (e.g. multiple
    concurrent sentences served due to an overlapping set of charges) and a multiple charges can
    reference a single Incarceration/Supervision Sentence (e.g. one sentence resulting from multiple
    charges). Thus, the relationship between StateCharge and each distinct Supervision/Incarceration
    Sentence type is many:many.
    """

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
class StateAssessment(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateAssessment object represents information about an
    assessment conducted for some person. Assessments are used in various stages
    of the justice system to assess a person's risk, or a person's needs, or to
    determine what course of action to take, such as pretrial sentencing or
    program reference.
    """

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
    assessment_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#40499): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40500): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 5018-05-17.
                    StateCode.US_IX,
                    # TODO(#40501): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 3013-04-30.
                    StateCode.US_PA,
                },
            ),
        ),
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
class StateSupervisionSentence(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionSentence object represents information about a single sentence to a period of
    supervision imposed as part of a group of related sentences. Multiple distinct, related sentences
    to supervision should be captured as separate supervision sentence objects within the same group.
    These sentences may, for example, be concurrent or consecutive to one another.
    Like the sentence group above, the supervision sentence represents only the imposition of some
    sentence terms, not an actual period of supervision experienced by the person.

    A StateSupervisionSentence object can reference many charges, and each charge can reference many
    sentences -- the relationship is many:many.

    A StateSupervisionSentence can have multiple child StateSupervisionPeriods. It can also have child
    StateIncarcerationPeriods since a sentence to supervision may result in a person's parole being
    revoked and the person being re-incarcerated, for example. In some jurisdictions, this would be
    modeled as distinct sentences of supervision and incarceration, but this is not universal.
    """

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
class StateIncarcerationSentence(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateIncarcerationSentence object represents information about a single sentence to a
    period of incarceration imposed as part of a group of related sentences. Multiple distinct, related
    sentences to incarceration should be captured as separate incarceration sentence objects within the same
    group. These sentences may, for example, be concurrent or consecutive to one another. Like the sentence
    group, the StateIncarcerationSentence represents only the imposition of some sentence terms,
    not an actual period of incarceration experienced by the person.
    A StateIncarcerationSentence can reference many charges, and each charge can reference many
    sentences -- the relationship is many:many.

    A StateIncarcerationSentence can have multiple child StateIncarcerationPeriods.
    It can also have child StateSupervisionPeriods since a sentence to incarceration may result in a person
    being paroled, for example. In some jurisdictions, this would be modeled as distinct sentences of
    incarceration and supervision, but this is not universal.
    """

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
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """
    The StateIncarcerationPeriod object represents information
    about a single period of incarceration, defined as a contiguous stay by a
    particular person in a particular facility. As a person transfers from
    facility to facility, these are modeled as multiple abutting
    incarceration periods. This also extends to temporary transfers to, say,
    hospitals or court appearances. The sequence of incarceration periods can
    be squashed into longer conceptual periods (e.g. from the first admission
    to the final release for a particular sentence) for analytical purposes,
    such as measuring recidivism and revocation -- this is done with a
    fine-grained examination of the admission dates, admission reasons,
    release dates, and release reasons of consecutive incarceration periods.

    Handling of incarceration periods is a crucial aspect of our
    platform and involves work in jurisdictional ingest mappings, entity
    matching, and calculation. Fortunately, this means that we have practice
    working with varied representations of this information.
    """

    # Type
    incarceration_type: Optional[StateIncarcerationType] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateIncarcerationType)
    )
    incarceration_type_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    admission_date: datetime.date = attr.ib(
        # TODO(#41434): Reset validator to just `attr_validators.is_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#41446): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1000-01-01.
                    StateCode.US_AR,
                    # TODO(#41448): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 8024-03-18.
                    StateCode.US_MI,
                    # TODO(#41449): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 0004-11-02.
                    StateCode.US_ND,
                },
            ),
        ),
    )
    release_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#41434): Reset validator to just `attr_validators.is_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#41447): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 4202-02-02.
                    StateCode.US_IX,
                    # TODO(#41448): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 8024-03-18.
                    StateCode.US_MI,
                },
            ),
        ),
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
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """
    The StateSupervisionPeriod object represents information about a
    single period of supervision, defined as a contiguous period of custody for a
    particular person under a particular jurisdiction. As a person transfers
    between supervising locations, these are modeled as multiple abutting
    supervision periods. Multiple periods of supervision for a particular person
    may be overlapping, due to extended periods of supervision that are
    temporarily interrupted by, say, periods of incarceration, or periods of
    supervision stemming from different charges.
    """

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
    start_date: datetime.date = attr.ib(
        # TODO(#41433): Reset validator to just `attr_validators.is_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#41438): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1000-01-01.
                    StateCode.US_AR,
                    # TODO(#41439): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 0204-07-10.
                    StateCode.US_AZ,
                    # TODO(#41441): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 9999-01-31.
                    StateCode.US_CA,
                    # TODO(#41442): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1873-11-28.
                    StateCode.US_MI,
                    # TODO(#41443): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 2924-10-01.
                    StateCode.US_PA,
                    # TODO(#41444): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 9994-04-21.
                    StateCode.US_TN,
                },
            ),
        )
    )

    termination_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#41433): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#41438): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1000-01-01.
                    StateCode.US_AR,
                    # TODO(#41439): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 2402-04-23.
                    StateCode.US_AZ,
                    # TODO(#41441): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 9999-01-30.
                    StateCode.US_CA,
                    # TODO(#41443): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 2924-10-01.
                    StateCode.US_PA,
                    # TODO(#41444): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 9999-12-31.
                    StateCode.US_TN,
                },
            ),
        ),
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
class StateSupervisionCaseTypeEntry(
    StateEntityMixin, EnumEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionCaseTypeEntry object represents a particular case type that applies to this
    period of supervision. A case type implies certain conditions of supervision that may apply, or
    certain levels or intensity of supervision, or certain kinds of specialized courts that
    generated the sentence to supervision, or even that the person being supervised may be
    supervised by particular kinds of officers with particular types of caseloads they are
    responsible for. A StateSupervisionPeriod may have zero to many distinct case types.
    """

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
class StateIncarcerationIncident(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateIncarcerationIncident object represents any behavioral incidents recorded against a
    person during a period of incarceration, such as a fight with another incarcerated individual
    or the possession of contraband. A StateIncarcerationIncident has zero to many
    StateIncarcerationIncidentOutcome children, indicating any official outcomes
    (e.g. disciplinary responses) due to the incident.
    """

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
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateIncarcerationIncidentOutcome object represents the outcomes in response to a particular
    StateIncarcerationIncident. These can be positive, neutral, or negative, but they should never
    be empty or null -- an incident that has no outcomes should simply have no
    StateIncarcerationIncidentOutcome children objects.
    """

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
    outcome_metadata: Optional[str] = attr.ib(
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
class StateSupervisionViolationTypeEntry(
    StateEntityMixin, EnumEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionViolationTypeEntry object represents each specific violation
    type that was composed within a single violation. Each supervision violation has
    zero to many such violation types. For example, a single violation may have been
    reported for both absconsion and a technical violation. However, it may also be
    the case that separate violations were recorded for both an absconsion and a
    technical violation which were related in the real world. The drawing line is
    how the violation is itself reported in the source data: if a single violation
    report filed by an agency staff member includes multiple types of violations,
    then it will be ingested into our schema as a single supervision violation with
    multiple supervision violation type entries.
    """

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
    StateEntityMixin, EnumEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionViolatedConditionEntry object represents a particular condition of supervision
    which was violated by a particular supervision violation. Each supervision violation has zero
    to many violated conditions. For example, a violation may be recorded because a brand new charge
    has been brought against the supervised person.
    """

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
class StateSupervisionViolation(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionViolation object represents any violations recorded against a person
    during a period of supervision, such as technical violation or a new offense. A
    StateSupervisionViolation has zero to many StateSupervisionViolationResponse children,
    indicating any official response to the violation, e.g. a disciplinary response such as a
    revocation back to prison or extension of supervision.
    """

    # Status
    # N/A

    # Attributes
    #   - When
    violation_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#40455): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40456): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 0019-07-02.
                    #  - Found dates as high as 2109-03-18.
                    StateCode.US_AR,
                    # TODO(#40464): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 3201-01-22.
                    StateCode.US_IX,
                    # TODO(#40462): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1004-02-02.
                    #  - Found dates as high as 9914-07-01.
                    StateCode.US_OR,
                    # TODO(#40463): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    #  - Found dates as high as 6200-06-15.
                    StateCode.US_PA,
                },
            ),
        ),
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
    violation_severity: Optional[StateSupervisionViolationSeverity] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionViolationSeverity),
    )
    violation_severity_raw_text: str | None = attr.ib(
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
    StateEntityMixin, EnumEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionViolationResponseDecisionEntry object represents each
    specific decision made in response to a particular supervision violation. Each
    supervision violation response has zero to many such decisions. Decisions are
    essentially the final consequences of a violation, actions such as continuance,
    privileges revoked, or revocation.
    """

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
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionViolationResponse object represents the official responses to a
    particular StateSupervisionViolation. These can be positive, neutral, or negative, but they
    should never be empty or null -- a violation that has no responses should simply have no
    StateSupervisionViolationResponse children objects.

    As described under StateIncarcerationPeriod, any StateSupervisionViolationResponse which
    leads to a revocation back to prison should be linked to the subsequent
    period of incarceration. This can be done implicitly in entity matching, or can
    be marked explicitly in incoming data, either here or
    on the incarceration period as the case may be.
    """

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
    response_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#40455): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40456): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 2913-10-22.
                    StateCode.US_AR,
                    # TODO(#40458): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 2105-12-30.
                    StateCode.US_CA,
                    # TODO(#40459): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1800-06-25.
                    #  - Found dates as high as 9994-12-31.
                    StateCode.US_MI,
                    # TODO(#40462): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1004-02-02.
                    #  - Found dates as high as 9914-07-01.
                    StateCode.US_OR,
                    # TODO(#40463): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    #  - Found dates as high as 2029-01-28.
                    StateCode.US_PA,
                },
            ),
        ),
    )

    #   - What
    is_draft: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )

    violation_response_metadata: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    violation_response_severity: Optional[
        StateSupervisionViolationResponseSeverity
    ] = attr.ib(
        default=None,
        validator=attr_validators.is_opt(StateSupervisionViolationResponseSeverity),
    )
    violation_response_severity_raw_text: str | None = attr.ib(
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
class StateProgramAssignment(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateProgramAssignment object represents information about
    the assignment of a person to some form of rehabilitative programming --
    and their participation in the program -- intended to address specific
    needs of the person. People can be assigned to programs while under
    various forms of custody, principally while incarcerated or under
    supervision. These programs can be administered by the
    agency/government, by a quasi-governmental organization, by a private
    third party, or any other number of service providers. The
    programming-related portion of our schema is still being constructed and
    will be added to in the near future.
    """

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
class StateEarlyDischarge(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateEarlyDischarge object represents a request and its associated decision to discharge
    a sentence before its expected end date. This includes various metadata surrounding the
    actual event of the early discharge request as well as who requested and approved the
    decision for early discharge. It is possible for a sentence to be discharged early without
    ending someone's supervision / incarceration term if that person is serving multiple sentences.
    """

    # Attributes
    #   - When
    request_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
    )
    decision_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
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
class StateSupervisionContact(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateSupervisionContact object represents information about a point of contact between a
    person under supervision and some agent representing the department, typically a
    supervising officer. These may include face-to-face meetings, phone calls, emails, or other
    such media. At these contacts, specific things may occur, such as referral to programming or
    written warnings or even arrest, but any and all events that happen as part of a single contact
    are modeled as one supervision contact. StateSupervisionPeriods have zero to many
    StateSupervisionContacts as children, and each StateSupervisionContact has one to many
    StateSupervisionPeriods as parents. This is because a given person may be serving multiple
    periods of supervision simultaneously in rare cases, and a given point of contact may apply
    to both.
    """

    # Status
    status: StateSupervisionContactStatus = attr.ib(
        validator=attr.validators.instance_of(StateSupervisionContactStatus)
    )
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    contact_date: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    scheduled_contact_date: Optional[datetime.date] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
            max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
        ),
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
class StateEmploymentPeriod(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateEmploymentPeriod object represents information about a
    person's employment status during a particular period of time. This
    object can be used to track employer information, or to track periods
    of unemployment if we have positive confirmation from the state that
    a person was unemployed at a given period.
    """

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
    start_date: datetime.date = attr.ib(
        # TODO(#42889): Reset validator to just `attr_validators.is_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#42890): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1000-01-01.
                    #  - Found dates as high as 9999-12-31.
                    StateCode.US_AR,
                    # TODO(#42891): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 2328-08-20.
                    StateCode.US_CA,
                    # TODO(#42892): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1108-11-08.
                    #  - Found dates as high as 9999-07-15.
                    StateCode.US_IX,
                    # TODO(#42894): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1780-01-01.
                    #  - Found dates as high as 9999-12-28.
                    StateCode.US_MI,
                    # TODO(#42895): Fix bad dates so all dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 0001-01-22.
                    #  - Found dates as high as 3012-08-30.
                    StateCode.US_UT,
                },
            ),
        )
    )
    end_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#42889): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#42890): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1000-01-01.
                    #  - Found dates as high as 9999-12-20.
                    StateCode.US_AR,
                    # TODO(#42892): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1900-01-01.
                    #  - Found dates as high as 4201-01-01.
                    StateCode.US_IX,
                    # TODO(#42894): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1753-01-01.
                    #  - Found dates as high as 9999-12-01.
                    StateCode.US_MI,
                    # TODO(#42895): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 0020-10-12.
                    #  - Found dates as high as 8201-09-12.
                    StateCode.US_UT,
                },
            ),
        ),
    )
    last_verified_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
            max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
        ),
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
class StateDrugScreen(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """The StateDrugScreen object represents information about a person's drug screen results for a given date."""

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
        # TODO(#40505): Reset validator to just `attr_validators.is_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40507): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 0010-01-02.
                    #  - Found dates as high as 9200-02-11.
                    StateCode.US_UT,
                },
            ),
        )
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
class StateTaskDeadline(
    StateEntityMixin, LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """
    The StateTaskDeadline object represents a single task that should be performed as
    part of someone’s supervision or incarceration term, along with an associated date
    that task can be started and/or a deadline when that task must be completed.
    """

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
        self.assert_datetime_less_than_or_equal(
            self.eligible_date,
            self.due_date,
            before_description="eligible_date",
            after_description="due_date",
        )


@attr.s(eq=False, kw_only=True)
class StatePersonStaffRelationshipPeriod(
    StateEntityMixin, Entity, BuildableAttr, DefaultableAttr, DurationMixin
):
    """The StatePersonStaffRelationshipPeriod object represents a period of time during
    which a staff member has a defined relationships with a justice impacted individual.

    That relationship might take place in the context of a specific location, in which
    case we can attribute a location to the relationship period as well.
    """

    # Type
    system_type: StateSystemType = attr.ib(
        validator=attr.validators.instance_of(StateSystemType),
    )
    system_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    relationship_type: StatePersonStaffRelationshipType = attr.ib(
        validator=attr.validators.instance_of(StatePersonStaffRelationshipType),
    )
    relationship_type_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    relationship_start_date: datetime.date = attr.ib(
        validator=attr_validators.is_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
    )
    relationship_end_date_exclusive: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
    )

    #   - Where
    # The county where this person is being supervised
    location_external_id: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    relationship_priority: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_positive_int
    )

    #   - Who
    # See |person| in entity relationships below.

    associated_staff_external_id: str = attr.ib(validator=attr_validators.is_str)
    associated_staff_external_id_type: str = attr.ib(validator=attr_validators.is_str)

    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    person_staff_relationship_period_id: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    person: Optional["StatePerson"] = attr.ib(default=None)

    @property
    def duration(self) -> DateRange:
        """Generates a DateRange for the days during which the relationship is valid.
        Since DateRange is never open, if the relationship is still active, then the
        exclusive upper bound of the range is set to tomorrow.
        """
        return DateRange.from_maybe_open_range(
            start_date=self.relationship_start_date, end_date=self.end_date_exclusive
        )

    @property
    def start_date_inclusive(self) -> datetime.date:
        return self.relationship_start_date

    @property
    def end_date_exclusive(self) -> datetime.date | None:
        return self.relationship_end_date_exclusive

    def __attrs_post_init__(self) -> None:
        # Disallow zero-day periods or periods where end date is before start date
        self.assert_datetime_less_than(
            before=self.relationship_start_date,
            after=self.relationship_end_date_exclusive,
            before_description="relationship_start_date",
            after_description="relationship_end_date_exclusive",
        )


@attr.s(eq=False, kw_only=True)
class StateStaffExternalId(
    StateEntityMixin, ExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    Each StateStaffExternalId holds a single external id for a given
    staff member provided by the source data system being ingested. An
    external id is a unique identifier for an individual, unique within
    the scope of the source data system. We include information denoting
    the source of the id to make this into a globally unique identifier.
    A staff member may have multiple StateStaffExternalId, but cannot
    have multiple with the same id_type.
    """

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
    StateEntityMixin,
    HasMultipleExternalIdsEntity[StateStaffExternalId],
    RootEntity,
    BuildableAttr,
    DefaultableAttr,
):
    """
    The StateStaff object represents some staff member operating on behalf of
    the criminal justice system, usually referenced in the context of taking
    some action related to a person moving through that system. This includes
    references such as the officers supervising people on parole, corrections
    officers overseeing people in prisons, people who manage those officers,
    and so on. This is not intended to be used to represent justice impacted
    individuals who are employed by the state as part of some work program.
    """

    # Attributes
    #   - What
    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    email: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_valid_email
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
class StateStaffRolePeriod(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    Represents information about a staff member's role in the justice system during a
    particular period of time.
    """

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(
        # TODO(#40469): Reset validator to just `attr_validators.is_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40470): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1111-01-01.
                    #  - Found dates as high as 3012-03-28.
                    StateCode.US_AR,
                    # TODO(#40471): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_ME,
                    # TODO(#40472): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_MI,
                    # TODO(#40473): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 0409-02-03.
                    #  - Found dates as high as 9001-10-01.
                    StateCode.US_MO,
                    # TODO(#40474): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_TN,
                },
            ),
        )
    )

    end_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#40469): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40470): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 2025-04-01.
                    StateCode.US_AR,
                    # TODO(#40473): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1007-09-14.
                    #  - Found dates as high as 2201-11-13.
                    StateCode.US_MO,
                    # TODO(#40474): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_TN,
                },
            ),
        ),
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
class StateStaffSupervisorPeriod(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    Represents information about a staff member’s direct supervisor during a
    particular period of time.
    """

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(
        # TODO(#40469): Reset validator to just `attr_validators.is_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40474): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_TN,
                },
            ),
        )
    )

    end_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
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
class StateStaffLocationPeriod(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    The StateStaffLocationPeriod models a period of time during which a
    staff member has a given assigned location. For now, this should be
    used to designate the staff member’s primary location, but may in the
    future be expanded to allow for multiple overlapping location periods
    for cases where staff members have primary, secondary etc locations.
    """

    # Attributes
    #   - When
    start_date: datetime.date = attr.ib(
        # TODO(#40469): Reset validator to just `attr_validators.is_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_date,
            state_exempted_validator(
                attr_validators.is_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40470): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 2025-04-07.
                    StateCode.US_AR,
                    # TODO(#40474): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_TN,
                },
            ),
        )
    )
    end_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#40469): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#40474): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01.
                    StateCode.US_TN,
                },
            ),
        ),
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
class StateStaffCaseloadTypePeriod(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    This table will have one row for each period in which one officer
    had a particular type of caseload. If the nature of their caseload
    changes over time, they will have more than one period
    reflecting the dates of those changes and what specialization, if any,
    corresponded to each period of their employment. Eventually, correctional
    officers who work in facilities will also be included in this table.
    """

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
    start_date: datetime.date = attr.ib(
        validator=attr_validators.is_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
    )

    # The end of the period where this officer had this type of specialized caseload
    end_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
    )
    # Primary key - Only optional when hydrated in the parsing layer, before we have
    # written this entity to the persistence layer
    staff_caseload_type_period_id: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Cross-entity relationships
    staff: Optional["StateStaff"] = attr.ib(default=None)


@attr.s(eq=False, kw_only=True)
class StateSentence(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    Represents a formal judgement imposed by the court that details the form of time served
    in response to the set of charges for which someone was convicted.
    This table contains all the attributes we can observe about the sentence at the time of sentence imposition,
    all of which will remain static over the course of the sentence being served.
    """

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
    imposed_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_not_future_date,
                exempted_states={
                    # TODO(#38698) Filter out imposed dates from the future
                    StateCode.US_IX,
                },
            ),
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#38805): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 2099-09-25.
                    StateCode.US_IX,
                    # TODO(#38802): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1875-09-25.
                    StateCode.US_MO,
                },
            ),
        ),
    )

    # This field should be hydrated if the state data provides an explicit piece of data
    # designating when they think a sentence begins serving (and/or accruing credit).
    # It should not be inferred from any data, including sentence statuses.
    # Future dates are not allowed because we check that this date aligns with serving statuses,
    # which cannot be in the future!
    current_state_provided_start_date: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
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

    def __attrs_post_init__(self) -> None:
        if len(self.parent_sentence_external_ids) != len(
            set(self.parent_sentence_external_ids)
        ):
            raise ValueError(
                f"{self.limited_pii_repr()} cannot have duplicate parent_sentence_external_ids"
            )
        if self.external_id in self.parent_sentence_external_ids:
            raise ValueError(
                f"{self.limited_pii_repr()} cannot list itself in its own parent_sentence_external_id_array"
            )

    @property
    def parent_sentence_external_ids(self) -> list[str]:
        """Returns the list of parent sentence external ids, or an empty list if there are none."""
        if self.parent_sentence_external_id_array:
            return self.parent_sentence_external_id_array.split(
                PARENT_SENTENCE_EXTERNAL_ID_SEPARATOR
            )
        return []

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="sentence_external_ids_unique_within_state",
                fields=["state_code", "external_id"],
            ),
        ]


@attr.s(eq=False, kw_only=True)
class StateChargeV2(
    StateEntityMixin, HasExternalIdEntity, BuildableAttr, DefaultableAttr
):
    """
    A formal allegation of an offense with information about the context for how that allegation was brought forth.
      - `date_charged` can be null for charges that have statuses like `DROPPED`.
      - `offense_date` can be null because of erroneous data from states

    TODO(#26240): Replace StateCharge with this entity
    """

    status: StateChargeV2Status = attr.ib(
        validator=attr.validators.instance_of(StateChargeV2Status)
    )
    status_raw_text: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Attributes
    #   - When
    offense_date: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_past_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#38800): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1015-10-12.
                    StateCode.US_AR,
                    # TODO(#38805): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 3502-01-01.
                    StateCode.US_IX,
                    # TODO(#38804): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as high as 2109-06-19.
                    StateCode.US_NE,
                },
            ),
        ),
    )
    date_charged: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_past_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND
        ),
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
    StateEntityMixin, LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """Represents a historical snapshot for when a given sentence had a given status."""

    # The start of the period of time over which the sentence status is valid
    status_update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_reasonable_past_datetime(
            min_allowed_datetime_inclusive=STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
        )
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
class StateSentenceLength(
    StateEntityMixin, LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """
    Represents a historical ledger of time attributes for a single sentence,
    including sentence length, accrued amount of days earned off, key dates, etc.

    The projected date fields for this entity can only be hydrated if the state provides
    sentence-specific estimates for individual sentences. Many states only provide projected
    dates for all sentences in a sentence group. If that is the case, hydrate the projected
    date fields on StateSentenceGroup.
    """

    # The start of the period of time over which the set of all sentence length attributes are valid
    length_update_datetime: datetime.datetime = attr.ib(
        # TODO(#38799): Reset validator to just `attr_validators.is_reasonable_past_datetime`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_not_future_datetime,
            state_exempted_validator(
                attr_validators.is_reasonable_past_datetime(
                    min_allowed_datetime_inclusive=STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
                ),
                exempted_states={
                    # TODO(#38800): Fix bad dates so all dates fall within the bounds (1900-01-02, <current date>).
                    #  - Found dates as low as 1900-01-01 00:00:00.
                    StateCode.US_AR,
                },
            ),
        )
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
    parole_eligibility_date_external: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#38800): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 3005-09-20.
                    StateCode.US_AR,
                },
            ),
        ),
    )
    # The date on which a person is projected to be released from incarceration to parole, if
    # they will be released to parole from the parent sentence.
    projected_parole_release_date_external: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
            max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
        ),
    )
    # The earliest date on which a person is projected to be released to liberty, if
    # they will be released to liberty from the parent sentence.
    projected_completion_date_min_external: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#38800): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 3005-09-20.
                    StateCode.US_AR,
                    # TODO(#38802): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 3301-02-25.
                    StateCode.US_MO,
                },
            ),
        ),
    )
    # The latest date on which a person is projected to be released to liberty, if
    # they will be released to liberty from the parent sentence.
    projected_completion_date_max_external: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=MAX_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#38800): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 3005-09-20.
                    StateCode.US_AR,
                    # TODO(#38802): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 9999-12-31.
                    StateCode.US_MO,
                    # TODO(#38804): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 2924-02-01.
                    StateCode.US_NE,
                },
            ),
        ),
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
        self.assert_datetime_less_than_or_equal(
            self.projected_parole_release_date_external,
            self.projected_completion_date_min_external,
            before_description="projected parole release",
            after_description="projected minimum completion",
        )
        self.assert_datetime_less_than_or_equal(
            self.projected_parole_release_date_external,
            self.projected_completion_date_max_external,
            before_description="projected parole release",
            after_description="projected maximum completion",
        )


@attr.s(eq=False, kw_only=True)
class StateSentenceGroup(
    StateEntityMixin, BuildableAttr, DefaultableAttr, HasExternalIdEntity
):
    """
    Represents a logical grouping of sentences that encompass an
    individual's interactions with a department of corrections.
    It begins with an individual's first sentence imposition and ends at liberty.
    This is a state agnostic term used by Recidiviz for a state
    specific administrative phenomena.

    StateSentenceGroup entities must be associated with a sentence
    (StateSentence.sentence_external_id) to be hydrated.
    """

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
    StateEntityMixin, LedgerEntityMixin, BuildableAttr, DefaultableAttr, Entity
):
    """Represents a historical ledger of attributes relating to a state designated group of sentences."""

    # The date when all sentence term attributes are updated
    group_update_datetime: datetime.datetime = attr.ib(
        validator=attr_validators.is_reasonable_past_datetime(
            min_allowed_datetime_inclusive=STANDARD_DATETIME_FIELD_REASONABLE_LOWER_BOUND
        ),
    )

    # The date on which a person is expected to become eligible for parole under the terms of this sentence
    parole_eligibility_date_external: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#38802): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1006-11-05.
                    #  - Found dates as high as 9994-02-28.
                    StateCode.US_MO,
                },
            ),
        ),
    )
    # The date on which a person is projected to be released from incarceration to parole
    projected_parole_release_date_external: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#38802): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as low as 1009-05-20.
                    #  - Found dates as high as 6201-03-23.
                    StateCode.US_MO,
                },
            ),
        ),
    )
    # The earliest date on which a person is projected to be released to liberty after having completed
    # all sentences in the term.
    projected_full_term_release_date_min_external: datetime.date | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt_reasonable_date(
            min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
            max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
        ),
    )
    # The latest date on which a person is projected to be released to liberty after having completed
    # all sentences in the term.
    projected_full_term_release_date_max_external: datetime.date | None = attr.ib(
        default=None,
        # TODO(#38799): Reset validator to just `attr_validators.is_opt_reasonable_date`
        #  once all state exemptions have been fixed.
        validator=attr.validators.and_(
            attr_validators.is_opt_date,
            state_exempted_validator(
                attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
                    max_allowed_date_exclusive=STANDARD_DATE_FIELD_REASONABLE_UPPER_BOUND,
                ),
                exempted_states={
                    # TODO(#38805): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 9999-12-31.
                    StateCode.US_IX,
                    # TODO(#38804): Fix bad dates so all non-null dates fall within the bounds (1900-01-02, 2300-01-01).
                    #  - Found dates as high as 2924-02-01.
                    StateCode.US_NE,
                },
            ),
        ),
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
        self.assert_datetime_less_than_or_equal(
            self.projected_parole_release_date_external,
            self.projected_full_term_release_date_min_external,
            before_description="projected parole release",
            after_description="projected minimum full term release",
        )
        self.assert_datetime_less_than_or_equal(
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
