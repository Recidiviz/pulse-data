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

from typing import Any, Callable, List, Tuple, Union

import attr

from recidiviz.persistence.entity.base_entity import EntityT
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)


@attr.s(repr=False, slots=True, hash=True)
class PreNormOptionalValidator:
    """A copy of _OptionalValidator from the attr library.
    We use this when we want an attribute to only be optional for pre-normalization."""

    # attr library does not expose the types of validators through their API
    # this validator can be any of attr.validators as well as this class.
    validator = attr.ib()  # type: ignore

    def __call__(self, inst: EntityT, attr_: attr.Attribute, value: Any) -> None:
        if not isinstance(inst, NormalizedStateEntity) and value is None:
            return
        self.validator(inst, attr_, value)

    def __repr__(self) -> str:
        return f"<pre-normalization optional validator for {self.validator!r} or None>"


def pre_norm_opt(
    validator: Union[Callable, List[Callable], Tuple[Callable]]
) -> Callable:
    """Returns a validator callable that is equivalent to attr.validators.is_optional"""
    return PreNormOptionalValidator(validator)
