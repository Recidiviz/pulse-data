# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Helper for running an attrs validator as a standalone pass/fail check,
outside of attrs class construction."""
from typing import Any

import attr

from recidiviz.common.attr_validators import AttrValidator


@attr.define(frozen=True, kw_only=True)
class _StandInAttrsClass:
    """Never instantiated; exists only so attr.fields() below can produce a
    real attr.Attribute to hand to validators run outside attrs construction."""

    some_attribute: Any = attr.ib()


_STAND_IN_ATTRIBUTE = attr.fields(_StandInAttrsClass).some_attribute


def passes_validator(validator: AttrValidator, value: Any) -> bool:
    """Returns whether |value| passes |validator|, which raises on an invalid
    value. Runs the validator against a stand-in instance and attribute so it
    can be used as a pass/fail predicate outside of attrs construction."""
    try:
        validator(None, _STAND_IN_ATTRIBUTE, value)
    except (ValueError, TypeError):
        return False
    return True
