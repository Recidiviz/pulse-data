# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Contains the StateSpecificSupervisionDelegate, the interface
for state-specific decisions involved in categorizing various attributes of
supervision."""
import abc
from typing import Optional, Tuple


class StateSpecificSupervisionDelegate(abc.ABC):
    """Interface for state-specific decisions involved in categorizing various attributes of supervision."""

    def supervision_types_mutually_exclusive(self) -> bool:
        """For some states, we want to track people on DUAL supervision as mutually exclusive from the groups of people on
        either PAROLE and PROBATION. For others, a person can be on multiple types of supervision simultaneously
        and contribute to counts for both types.

        Default behavior is that supervision types are *not* mutually exclusive, meaning a person can be on multiple types of supervision simultaneously.

        Returns whether our calculations should consider supervision types as distinct for the given state_code.
        """
        return False

    def supervision_location_from_supervision_site(
        self, supervision_site: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Retrieves level 1 and level 2 location information from a supervision site.
        By default, returns the |supervision_site| as the level 1 location, and None as the level 2 location.
        """
        # TODO(#3829): Remove this helper once we've once we've built level 1/level 2 supervision
        #  location distinction directly into our schema.
        level_1_supervision_location = supervision_site
        level_2_supervision_location = None

        return (
            level_1_supervision_location or None,
            level_2_supervision_location or None,
        )
