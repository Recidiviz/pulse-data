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
"""Defines a wrapper around any attr field validator that can be used to exempt certain
states from the validation.
"""
from typing import Any, Callable, Set

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.types import T, assert_type


def state_exempted_validator(
    validator: Callable[[Any, attr.Attribute, T], None],
    *,
    exempted_states: Set[StateCode],
    exempted_state_validator: Callable[[Any, attr.Attribute, T], None] | None,
) -> Callable[[Any, attr.Attribute, T], None]:
    """A wrapper around any attr field validator that can be used to exempt certain
    states from the validation. In order to use this validator, the class with the field
    you're validating must also have a hydrated state_code field.

    Args:
        validator: The validator to run for non-exempted states.
        exempted_states: Set of state codes that are exempt from the primary validator.
        exempted_state_validator: Optional fallback validator to run for exempted states.
            If not provided, no validation is performed for exempted states.
    """

    def _wrapper(instance: Any, attribute: attr.Attribute, value: T) -> None:
        if not hasattr(instance, "state_code"):
            raise ValueError(f"Class [{type(instance)}] does not have state_code")

        state_code = StateCode(assert_type(getattr(instance, "state_code"), str))

        if state_code in exempted_states:
            if exempted_state_validator is not None:
                exempted_state_validator(instance, attribute, value)
        else:
            validator(instance, attribute, value)

    return _wrapper
