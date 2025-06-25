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
"""This file contains code to validate fields specifically for state entities."""

from typing import Any, Callable, List, Optional, Tuple, Union

import attr
from more_itertools import one

from recidiviz.persistence.entity.base_entity import EntityT
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)


@attr.s(repr=False, slots=True, hash=True)
class ParsingOptionalOnlyValidator:
    """A copy of _OptionalValidator from the attr library.
    We use this when we want an attribute to only be optional for entities produced
    by the ingest view entity creation step, but should never be null once we have
    merged entity trees together.

    Any field marked with this validator will be confirmed nonnull in the RunValidations
    step of ingest pipelines.
    """

    # attr library does not expose the types of validators through their API
    # this validator can be any of attr.validators as well as this class.
    validator = attr.ib()  # type: ignore

    def __call__(self, inst: EntityT, attr_: attr.Attribute, value: Any) -> None:
        if isinstance(inst, NormalizedStateEntity):
            raise TypeError(
                f"Cannot use a parsing_opt_only() validator on "
                f"NormalizedStateEntity classes. Found on [{type(inst).__name__}] "
                f"field [{attr_.name}]"
            )

        if value is None:
            return
        self.validator(inst, attr_, value)

    def __repr__(self) -> str:
        return f"<pre-merge optional validator for {self.validator!r} or None>"


def parsing_opt_only(
    validator: Union[Callable, List[Callable], Tuple[Callable]],
) -> Callable:
    """Returns a validator callable that is equivalent to attr.validators.is_optional.

    We use this when we want an attribute to only be optional for entities produced
    by the ingest view entity creation step, but should never be null once we have
    merged entity trees together.

    Any field marked with this validator will be confirmed nonnull in the RunValidations
    step of ingest pipelines.
    """
    return ParsingOptionalOnlyValidator(validator)


class AppearsWithValidator:
    """
    Validator to ensure that a specified field appears together with the current field.

    This validator enforces that both fields are either set (non-None) or unset (None) together,
    and ensures that the specified related field also includes an `AppearsWithValidator`
    referencing the current field.
    """

    def __init__(self, field_name: str):
        self.field_name = field_name

    def __call__(
        self, instance: Any, attribute: attr.Attribute, value: Optional[Any]
    ) -> None:
        if not hasattr(instance, self.field_name):
            raise ValueError(
                f"{self.field_name} is currently not an attribute of {type(instance)}. "
                f"Fields '{self.field_name}' and '{attribute.name}' should both be attributes of {type(instance)}"
            )

        other_field = one(
            f for f in attr.fields(instance.__class__) if f.name == self.field_name
        )

        if repr(AppearsWithValidator(attribute.name)) not in str(other_field.validator):
            raise ValueError(
                f"Field {self.field_name} does not have 'appears_with' validator"
            )

        other_value = getattr(instance, self.field_name)
        if (value is None) != (other_value is None):
            raise ValueError(
                f"Fields of {type(instance)}: '{attribute.name}' and '{self.field_name}' must both be set or both be None. "
                f"Current values: {attribute.name}={value}, {self.field_name}={other_value}"
            )

    def __repr__(self) -> str:
        return f"AppearsWithValidator field_name:{self.field_name}"


def appears_with(field_name: str) -> AppearsWithValidator:
    return AppearsWithValidator(field_name)
