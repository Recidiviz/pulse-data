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
   LookML dashboard."""

import abc
from enum import Enum

import attr


class LookMLDashboardLayoutType(Enum):
    NEWSPAPER = "newspaper"
    GRID = "grid"
    STATIC = "static"
    TILE = "tile"


@attr.define
class LookMLDashboardParameter:
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
    def title(cls, title: str) -> "LookMLDashboardParameter":
        return DashboardParameterTitle(title)

    @classmethod
    def description(cls, description: str) -> "LookMLDashboardParameter":
        return DashboardParameterDescription(description)

    @classmethod
    def layout(
        cls, layout_type: LookMLDashboardLayoutType
    ) -> "LookMLDashboardParameter":
        return DashboardParameterLayout(layout_type)


@attr.define
class DashboardParameterTitle(LookMLDashboardParameter):
    """Generates a `title` parameter for a dashboard (see
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#title_for_dashboard)
    """

    text: str

    @property
    def key(self) -> str:
        return "title"

    @property
    def value_text(self) -> str:
        return self.text


@attr.define
class DashboardParameterDescription(LookMLDashboardParameter):
    """Generates a `description` parameter for a dashboard (see
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#description_for_dashboard)
    """

    text: str

    @property
    def key(self) -> str:
        return "description"

    @property
    def value_text(self) -> str:
        return self.text


@attr.define
class DashboardParameterLayout(LookMLDashboardParameter):
    """Generates a `layout` parameter for a dashboard (see
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#layout)
    """

    layout_type: LookMLDashboardLayoutType

    @property
    def key(self) -> str:
        return "layout"

    @property
    def value_text(self) -> str:
        return self.layout_type.value
