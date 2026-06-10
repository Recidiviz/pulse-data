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
"""Validators specific to the `pulse-data` codebase."""
from typing import Any

import attr

from recidiviz.common.attr_validators import is_str
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.string import is_meaningful_docstring

# TODO(#84110) add is_valid_project_id


def is_meaningful_description(
    instance: Any, attribute: attr.Attribute, value: str
) -> None:
    """Validator to ensure that a field description is meaningful (i.e. not empty or just whitespace)."""
    is_str(instance, attribute, value)

    if not is_meaningful_docstring(value):
        raise ValueError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] must have "
            f"a meaningful description. Found value [{value}]"
        )


is_state_code = attr.validators.instance_of(StateCode)
