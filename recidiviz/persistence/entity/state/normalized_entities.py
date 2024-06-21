# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Classes for the entities that we normalize."""
from typing import Any, Callable, Dict, List, Optional, Type

import attr

from recidiviz.common.attr_utils import get_non_flat_attribute_class_name, is_list
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.date import NonNegativeDateRange
from recidiviz.persistence.entity.base_entity import Entity

# TODO(#30075): Deprecate the entities in file in favor of the entities defined in
#  normalized_entities_v2.py.
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import SequencedEntityMixin


def is_normalized_entity_validator(
    instance: NormalizedStateEntity, attribute: attr.Attribute, value: Any
) -> None:
    """Asserts that the type of the |value| is a NormalizedStateEntity. If the
    |value| is a list, asserts that each individual element is of type
    NormalizedStateEntity."""
    if is_list(attribute):
        for v in value:
            if not isinstance(v, NormalizedStateEntity):
                raise TypeError(
                    f"The {attribute.name} field on the {instance.__class__.__name__} "
                    f"class must store the Normalized version of the entity. "
                    f"Found: {v.__class__.__name__}."
                )
    elif value and not isinstance(value, NormalizedStateEntity):
        raise TypeError(
            f"The {attribute.name} field on the {instance.__class__.__name__} "
            f"class must store the Normalized version of the entity. "
            f"Found: {value.__class__.__name__}."
        )


def get_entity_class_names_excluded_from_normalization() -> List[str]:
    """Returns the names of all entity classes that are never modified by
    normalization.

    We never normalize the StatePerson / StateStaff entity.
    """
    return [
        state_entities.StatePerson.__name__,
        state_entities.StateStaff.__name__,
    ]


def _get_ref_fields_with_reference_class_names(
    cls: Type, class_names_to_ignore: Optional[List[str]] = None
) -> Dict[str, str]:
    """Returns a dictionary mapping each field on the class that is a forward ref to
    the class name referenced in the attribute."""
    class_names_to_ignore = class_names_to_ignore or []

    return_value: Dict[str, str] = {}
    for field, attribute in attr.fields_dict(cls).items():
        referenced_cls_name = get_non_flat_attribute_class_name(attribute)
        if referenced_cls_name and referenced_cls_name not in class_names_to_ignore:
            return_value[field] = referenced_cls_name

    return return_value


def add_normalized_entity_validator_to_ref_fields(
    normalized_class: Type, fields: List[attr.Attribute]
) -> List[attr.Attribute]:
    """Updates the validator of any attribute on the class that is a reference to
    another entity to assert that the type of entity stored in the attribute is the
    Normalized version of the entity."""
    updated_fields: List[attr.Attribute] = []

    if not issubclass(normalized_class, NormalizedStateEntity):
        raise ValueError(
            "add_normalized_entity_validator_to_ref_fields should only "
            "be used as a field_transformer for classes of type "
            f"NormalizedStateEntity. Found {normalized_class}."
        )

    if not issubclass(normalized_class, Entity):
        raise ValueError(
            f"Normalized class [{normalized_class}] does not inherit from Entity."
        )

    ref_field_names = _get_ref_fields_with_reference_class_names(
        normalized_class,
        class_names_to_ignore=get_entity_class_names_excluded_from_normalization(),
    )

    for field in fields:
        if field.name in ref_field_names:
            updated_validators: List[Callable] = [is_normalized_entity_validator]

            if field.validator:
                updated_validators.append(field.validator)

            updated_fields.append(
                field.evolve(validator=attr.validators.and_(*updated_validators))
            )
        else:
            updated_fields.append(field)

    return updated_fields


# StateAssessment subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateAssessment(
    state_entities.StateAssessment, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateAssessment entities that have been normalized and are
    prepared to be used in calculations."""

    # A string representing an interval category based on assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # StateStaff id foreign key for the conducting officer
    conducting_staff_id: Optional[int] = attr.ib(default=None)


# StateIncarcerationPeriod subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateIncarcerationPeriod(
    state_entities.StateIncarcerationPeriod, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateIncarcerationPeriod entities that have been
    normalized and are prepared to be used in calculations."""

    purpose_for_incarceration_subtype: Optional[str] = attr.ib(default=None)

    incarceration_admission_violation_type: Optional[
        StateSupervisionViolationType
    ] = attr.ib(default=None)

    @property
    def duration(self) -> NonNegativeDateRange:
        duration_unsafe = super().duration
        return NonNegativeDateRange(
            duration_unsafe.lower_bound_inclusive_date,
            duration_unsafe.upper_bound_exclusive_date,
        )


# StateProgramAssignment subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateProgramAssignment(
    state_entities.StateProgramAssignment, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateProgramAssignment entities that have been
    normalized and are prepared to be used in calculations."""

    # StateStaff id foreign key for the referring officer
    referring_staff_id: Optional[int] = attr.ib(default=None)


# StateSupervisionContact subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionContact(
    state_entities.StateSupervisionContact, NormalizedStateEntity
):
    """Stores instances of StateSupervisionContact entities that have been
    normalized and are prepared to be used in calculations."""

    # StateStaff id foreign key for the contacting officer
    contacting_staff_id: Optional[int] = attr.ib(default=None)


# StateSupervisionPeriod subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionPeriod(
    state_entities.StateSupervisionPeriod, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateSupervisionPeriod entities that have been
    normalized and are prepared to be used in calculations."""

    # StateStaff id foreign key for the supervising officer
    supervising_officer_staff_id: Optional[int] = attr.ib(default=None)

    @property
    def duration(self) -> NonNegativeDateRange:
        duration_unsafe = super().duration
        return NonNegativeDateRange(
            duration_unsafe.lower_bound_inclusive_date,
            duration_unsafe.upper_bound_exclusive_date,
        )


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionCaseTypeEntry(
    state_entities.StateSupervisionCaseTypeEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionCaseTypeEntry entities that have been
    normalized and are prepared to be used in calculations."""


# StateSupervisionViolation subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolation(
    state_entities.StateSupervisionViolation, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolation entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolationTypeEntry(
    state_entities.StateSupervisionViolationTypeEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolationTypeEntry entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolatedConditionEntry(
    state_entities.StateSupervisionViolatedConditionEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolatedConditionEntry entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolationResponse(
    state_entities.StateSupervisionViolationResponse,
    NormalizedStateEntity,
    SequencedEntityMixin,
):
    """Stores instances of StateSupervisionViolationResponse entities that have been
    normalized and are prepared to be used in calculations."""

    # StateStaff id foreign key for the deciding officer
    deciding_staff_id: Optional[int] = attr.ib(default=None)


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolationResponseDecisionEntry(
    state_entities.StateSupervisionViolationResponseDecisionEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolationResponseDecisionEntry entities that have been
    normalized and are prepared to be used in calculations."""


# StateIncarcerationSentence / StateSupervisionSentence subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateIncarcerationSentence(
    state_entities.StateIncarcerationSentence, NormalizedStateEntity
):
    """Stores instances of StateIncarcerationSentence entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionSentence(
    state_entities.StateSupervisionSentence, NormalizedStateEntity
):
    """Stores instances of StateSupervisionSentence entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateCharge(state_entities.StateCharge, NormalizedStateEntity):
    """Stores instances of StateCharge entities that have been
    normalized and are prepared to be used in calculations."""

    # The original, state-provided NCIC (National Crime Information Center) code for
    # this offense.
    ncic_code_external: Optional[str] = attr.ib(default=None)

    # A high-level category associated with the state-provided NCIC code (e.g.
    # Kidnapping, Bribery, etc).
    ncic_category_external: Optional[str] = attr.ib(default=None)

    # The human-readable description associated with the offense, as indicated by state
    # data or as inferred from the state-provided NCIC code.
    description_external: Optional[str] = attr.ib(default=None)

    # Whether the charge is for a violent offense, as indicated by state data or as
    # inferred from the state-provided NCIC code.
    is_violent_external: Optional[bool] = attr.ib(default=None)

    # Whether the charge is for a drug-related offense, as indicated by state data or as
    # inferred from the state-provided NCIC code.
    is_drug_external: Optional[bool] = attr.ib(default=None)

    # Whether the charge is classified as sex offense, as indicated by state data or as
    # inferred by the state-provided NCIC code.
    is_sex_offense_external: Optional[bool] = attr.ib(default=None)


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateEarlyDischarge(
    state_entities.StateEarlyDischarge, NormalizedStateEntity
):
    """Stores instances of StateEarlyDischarge entities that have been normalized
    and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateStaffRolePeriod(
    state_entities.StateStaffRolePeriod, NormalizedStateEntity
):
    """Stores instances of StateStaffRolePeriod entities that have been normalized
    and are prepared to be used in calculations."""
