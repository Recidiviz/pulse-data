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
"""One whole name recorded for a person, reduced to the components the
conflict check compares."""
import enum

import attr

from recidiviz.common import attr_validators


class NameComponent(enum.Enum):
    """A component of a person's name that is compared on its own."""

    SURNAME = "surname"
    GIVEN_NAME = "given_name"
    MIDDLE_NAME = "middle_name"


@attr.define(frozen=True, kw_only=True)
class Name:
    """One whole name recorded for a person -- a fragment's primary name or one
    of the aliases it records -- reduced to the components the conflict check
    compares. Kept whole so an alias is compared as a name rather than as a bag
    of interchangeable components. Any component the source did not record is
    None. Mirrors the Identity Service's Name, like IdentityName and
    IdentityAlias do.
    """

    surname: str | None = attr.ib(validator=attr_validators.is_opt_str)
    given_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    middle_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    name_suffix: str | None = attr.ib(validator=attr_validators.is_opt_str)

    def value_for(self, component: NameComponent) -> str | None:
        """Returns this name's value for one component."""
        if component is NameComponent.SURNAME:
            return self.surname
        if component is NameComponent.GIVEN_NAME:
            return self.given_name
        if component is NameComponent.MIDDLE_NAME:
            return self.middle_name
        raise ValueError(f"Unexpected component: [{component}]")
