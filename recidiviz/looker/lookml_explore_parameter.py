# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines a class representing a parameter to be added to a 
   LookML explore."""

import abc

import attr


@attr.define
class LookMLExploreParameter:
    """Defines a LookML field parameter, including the parameter key
    and value string and attributes specific to the subclass"""

    @property
    @abc.abstractmethod
    def key(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def value_text(self) -> str:
        pass

    def build(self) -> str:
        return f"{self.key}: {self.value_text}"

    @classmethod
    def description(cls, description: str) -> "LookMLExploreParameter":
        return ExploreParameterDescription(text=description)

    @classmethod
    def group_label(cls, group_label: str) -> "LookMLExploreParameter":
        return ExploreParameterGroupLabel(text=group_label)


# DISPLAY PARAMETERS
@attr.define
class ExploreParameterDescription(LookMLExploreParameter):
    """Generates a `description` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-explore-description).
    """

    text: str

    @property
    def key(self) -> str:
        return "description"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'


@attr.define
class ExploreParameterGroupLabel(LookMLExploreParameter):
    """Generates a `group_label` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-explore-group-label).
    """

    text: str

    @property
    def key(self) -> str:
        return "group_label"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'
