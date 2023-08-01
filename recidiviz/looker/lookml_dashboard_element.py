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
"""Defines a class representing a element as part of the `elements`
   parameter on a LookML Dashboard, which define parts of the dashboard:
   data visualizations, text tiles, and buttons.

   Documentation:
   https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-element
   """
from enum import Enum
from typing import Dict, List, Optional

import attr


class LookMLElementType(Enum):
    """
    Represents the `type` field of a dashboard element:
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-element
    """

    LOOKER_GRID = "looker_grid"
    # Add more types here if generating them is necessary


class LookMLNoteDisplayType(Enum):
    """
    Represents the `note_display` field of a dashboard element
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-table-chart#note_display
    """

    ABOVE = "above"
    BELOW = "below"
    HOVER = "hover"


@attr.define
class LookMLListen:
    """
    Wraps a mapping between filter names and view field names to describe
    a `listen` parameter for a dashboard element. Documentation:
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-table-chart#listen
    """

    listens: Dict[str, str]

    def __attrs_post_init__(self) -> None:
        if any("." not in f for f in self.listens.values()):
            raise ValueError(
                f"LookML element is listening to field names {self.listens.values()},"
                f"but field names should be fully scoped: use view_name.field_name,"
                f"not just field_name"
            )

    def build(self) -> str:
        """
        Return a formatted string representing this listen parameter
        """
        formatted_listens = (
            f"{filter_name}: {field_name}"
            for filter_name, field_name in self.listens.items()
        )
        return "\n      " + "\n      ".join(formatted_listens)


@attr.define
class LookMLDashboardElement:
    """Generates a `element` parameter for a dashboard,
    which allow the user to change what data is shown on the dashboard.
    Documentation: https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#elements
    """

    name: str
    title: Optional[str] = attr.field(default=None)
    explore: Optional[str] = attr.field(default=None)
    model: Optional[str] = attr.field(default=None)
    type: Optional[LookMLElementType] = attr.field(default=None)
    fields: Optional[List[str]] = attr.field(default=None)
    sorts: Optional[List[str]] = attr.field(default=None)
    note_display: Optional[LookMLNoteDisplayType] = attr.field(default=None)
    note_text: Optional[str] = attr.field(default=None)
    listen: Optional[LookMLListen] = attr.field(default=None)
    row: Optional[int] = attr.field(default=None)
    col: Optional[int] = attr.field(default=None)
    width: Optional[int] = attr.field(default=None)
    height: Optional[int] = attr.field(default=None)

    def __attrs_post_init__(self) -> None:
        if self.fields and any("." not in f for f in self.fields):
            raise ValueError(
                f"LookML element {self.name} has the list of fields {self.fields},"
                f"but field names should be fully scoped: use view_name.field_name,"
                f"not just field_name"
            )

        if self.sorts and any("." not in s for s in self.sorts):
            raise ValueError(
                f"LookML element {self.name} has the list of sorts {self.sorts},"
                f"but field names should be fully scoped: use view_name.field_name,"
                f"not just field_name"
            )

    def build(self) -> str:
        """
        Return a formatted string representing the element itself.
        """
        formatted_values = []
        for attribute in attr.fields_dict(self.__class__):
            attr_value = getattr(self, attribute)
            # skip attributes that were not provided
            if attr_value is not None:
                if isinstance(attr_value, str):
                    formatted_value = attr_value
                elif isinstance(attr_value, int):
                    formatted_value = str(attr_value)
                elif isinstance(attr_value, Enum):
                    formatted_value = attr_value.value
                elif isinstance(attr_value, LookMLListen):
                    formatted_value = attr_value.build()
                elif isinstance(attr_value, List):
                    formatted_value = "[" + ",\n      ".join(attr_value) + "]"
                else:
                    raise ValueError(
                        f"Unexpected value provided for attribute {attribute} in LookML Dashboard element {self.name}"
                    )
                formatted_values.append(f"{attribute}: {formatted_value}")

        element_contents = "\n    ".join(formatted_values)
        return "- " + element_contents
