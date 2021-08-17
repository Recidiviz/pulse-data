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


class StateSpecificSupervisionDelegate(abc.ABC):
    """Interface for state-specific decisions involved in categorizing various attributes of supervision."""

    def supervision_types_mutually_exclusive(self) -> bool:
        """For some states, we want to track people on DUAL supervision as mutually exclusive from the groups of people on
        either PAROLE and PROBATION. For others, a person can be on multiple types of supervision simultaneously
        and contribute to counts for both types.

        Returns whether our calculations should consider supervision types as distinct for the given state_code.
        """
        return False
