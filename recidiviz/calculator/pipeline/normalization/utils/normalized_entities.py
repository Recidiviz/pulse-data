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

from recidiviz.calculator.pipeline.utils.execution_utils import (
    get_entity_class_names_excluded_from_pipelines,
)
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import get_non_flat_attribute_class_name, is_list
from recidiviz.common.date import NonNegativeDateRange
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StatePerson,
    StateProgramAssignment,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)


class NormalizedStateEntity(BuildableAttr):
    """Models an entity in state/entities.py that has been normalized and is prepared
    to be used in calculations."""

    @classmethod
    def base_class_name(cls) -> str:
        """The name of the base state entity that this normalized entity extends."""
        return cls.__base__.__name__


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

    We never normalize the StatePerson entity, and classes that are excluded from
    pipelines cannot be normalized.
    """
    return [StatePerson.__name__] + get_entity_class_names_excluded_from_pipelines()


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


@attr.s
class SequencedEntityMixin:
    """Set of attributes for a normalized entity that can be ordered in a sequence."""

    sequence_num: int = attr.ib()


# StateIncarcerationPeriod subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateIncarcerationPeriod(
    StateIncarcerationPeriod, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateIncarcerationPeriod entities that have been
    normalized and are prepared to be used in calculations."""

    purpose_for_incarceration_subtype: Optional[str] = attr.ib(default=None)

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
    StateProgramAssignment, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateProgramAssignment entities that have been
    normalized and are prepared to be used in calculations."""


# StateSupervisionPeriod subtree
@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionPeriod(
    StateSupervisionPeriod, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateSupervisionPeriod entities that have been
    normalized and are prepared to be used in calculations."""

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
    StateSupervisionCaseTypeEntry, NormalizedStateEntity
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
    StateSupervisionViolation, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolation entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolationTypeEntry(
    StateSupervisionViolationTypeEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolationTypeEntry entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolatedConditionEntry(
    StateSupervisionViolatedConditionEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolatedConditionEntry entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolationResponse(
    StateSupervisionViolationResponse, NormalizedStateEntity, SequencedEntityMixin
):
    """Stores instances of StateSupervisionViolationResponse entities that have been
    normalized and are prepared to be used in calculations."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class NormalizedStateSupervisionViolationResponseDecisionEntry(
    StateSupervisionViolationResponseDecisionEntry, NormalizedStateEntity
):
    """Stores instances of StateSupervisionViolationResponseDecisionEntry entities that have been
    normalized and are prepared to be used in calculations."""
