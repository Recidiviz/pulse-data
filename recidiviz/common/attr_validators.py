# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Contains helper aliases and functions for various attrs validators that can be passed to the `validator=` arg of
any attr field. For example:

@attr.s
class MyClass:
  name: Optional[str] = attr.ib(validator=is_opt(str))
  is_valid: bool = attr.ib(validator=is_bool)
"""

import datetime
from typing import Any, Type, Callable

import attr


def is_opt(cls_type: Type) -> Callable:
    """Returns an attrs validator that checks if the value is an instance of |cls_type| or None."""
    return attr.validators.optional(attr.validators.instance_of(cls_type))


def is_non_empty_str(_instance: Any, _attribute: attr.Attribute, value: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f'Expected value type str, found {type(str)}.')
    if not value:
        raise ValueError('String value should not be empty.')


# String field validators
is_str = attr.validators.instance_of(str)
is_opt_str = is_opt(str)

# Int field validators
is_int = attr.validators.instance_of(int)
is_opt_int = is_opt(int)

# Date field validators
is_date = attr.validators.instance_of(datetime.date)
is_opt_date = is_opt(datetime.date)

# Boolean field validators
is_bool = attr.validators.instance_of(bool)
is_opt_bool = is_opt(bool)

# List field validators
is_list = attr.validators.instance_of(list)
