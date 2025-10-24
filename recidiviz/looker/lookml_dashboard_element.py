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

from recidiviz.common import attr_validators
from recidiviz.looker.lookml_dashboard_filter import LookMLDashboardFilter
from recidiviz.looker.lookml_view import LookMLView

FULL_SCREEN_WIDTH = 24
SPLIT_SCREEN_WIDTH = int(FULL_SCREEN_WIDTH / 2)
DEFAULT_HEIGHT = 6
SMALL_ELEMENT_HEIGHT = 2


def dict_to_scoped_field_names(d: dict[str, list[str]]) -> list[str]:
    """Converts a dictionary of view names to field names to a list of fully scoped field names.

    ex:
    {
        "state_person": {
            "person_id",
            "state_code",
        },
        "state_person_external_id": {
            "external_id",
        },
    } -> ["state_person.person_id", "state_person.state_code", "state_person_external_id.external_id"]
    """
    return [f"{view}.{field}" for view, fields in d.items() for field in fields]


class LookMLElementType(Enum):
    """
    Represents the `type` field of a dashboard element:
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-element
    """

    LOOKER_GRID = "looker_grid"
    LOOKER_TIMELINE = "looker_timeline"
    SINGLE_VALUE = "single_value"
    TEXT = "text"
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

    @classmethod
    def from_fields(cls, fields: List[str]) -> "LookMLListen":
        """Create a LookMLListen object from a list of fully scoped field names in the
        format view_name.field_name"""

        return cls(
            {LookMLDashboardFilter.to_filter_name(field): field for field in fields}
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
class LookMLSort:
    """
    Represents a sort parameter for a dashboard element.
    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-table-chart#sorts
    """

    field: str
    desc: Optional[bool] = attr.field(default=False)

    def __attrs_post_init__(self) -> None:
        if "." not in self.field:
            raise ValueError(
                f"LookML sort field {self.field} should be fully scoped: use view_name.field_name,"
                f"not just field_name"
            )

    def build(self) -> str:
        """
        Return a formatted string representing this sort parameter
        """
        return f"{self.field}{' desc' if self.desc else ''}"


@attr.define
class LookMLColorApplication:
    """
    Represents a color application parameter for a dashboard element.

    https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-table-chart#color_application
    """

    collection_id: str = attr.field(validator=attr_validators.is_non_empty_str)
    palette_id: str = attr.field(validator=attr_validators.is_non_empty_str)

    def build(self) -> str:
        """Return a formatted string representing this color application parameter"""
        formatted_values = []
        for attribute in attr.fields_dict(self.__class__):
            attr_value = getattr(self, attribute)
            if attr_value is not None:
                if isinstance(attr_value, str):
                    formatted_value = attr_value
                elif isinstance(attr_value, dict):
                    formatted_value = "".join(
                        f"\n        {k}: {v}" for k, v in attr_value.items()
                    )
                else:
                    raise ValueError(
                        f"Unexpected value provided for attribute {attribute} in LookML Color Application {self.collection_id}"
                    )
                formatted_values.append(f"{attribute}: {formatted_value}")
        return "\n      " + "\n      ".join(formatted_values)


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
    sorts: Optional[List[LookMLSort]] = attr.field(default=None)
    note_display: Optional[LookMLNoteDisplayType] = attr.field(default=None)
    note_text: Optional[str] = attr.field(default=None)
    body_text: Optional[str] = attr.field(default=None)
    listen: Optional[LookMLListen] = attr.field(default=None)
    show_legend: Optional[bool] = attr.field(default=None)
    group_bars: Optional[bool] = attr.field(default=None)
    color_application: Optional[LookMLColorApplication] = attr.field(default=None)
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
        self._validate_sorts()

    def _validate_sorts(self) -> None:
        """Validates that all sort fields are defined in the fields list."""
        if self.sorts is None:
            return

        if not self.fields:
            raise ValueError(
                f"Sorts are defined for element [{self.name}] but fields are not."
            )

        for sort in self.sorts:
            if sort.field not in self.fields:
                raise ValueError(
                    f"Sort field [{sort.field}] is not defined in element [{self.name}]."
                )

    @classmethod
    def for_table_chart(
        cls,
        name: str,
        explore: Optional[str] = None,
        model: Optional[str] = None,
        listen: Optional[LookMLListen] = None,
        fields: Optional[List[str]] = None,
        sorts: Optional[List[LookMLSort]] = None,
    ) -> "LookMLDashboardElement":
        """
        Create a LookMLDashboardElement object for a table chart. If you want to add more arguments,
        add them here and update the build method accordingly.
        """
        return cls(
            name=name,
            title=name,
            explore=explore,
            model=model,
            type=LookMLElementType.LOOKER_GRID,
            listen=listen,
            fields=fields,
            sorts=sorts,
        )

    def validate_referenced_fields_exist_in_views(
        self, views: list[LookMLView]
    ) -> None:
        """Ensures all referenced fields exist in the provided LookML views."""
        if self.fields is None:
            return

        view_name_to_field_names: dict[str, set[str]] = {
            view.view_name: view.field_names for view in views
        }
        for field in self.fields:
            view, field_name = field.split(".", 1)
            if view not in view_name_to_field_names:
                raise ValueError(f"View [{view}] is not defined.")

            if field_name not in view_name_to_field_names[view]:
                raise ValueError(
                    f"Field [{field_name}] is not defined in view [{view}]."
                    f" Valid fields: [{view_name_to_field_names[view]}]"
                )

    @property
    def listens(self) -> Optional[Dict[str, str]]:
        """Returns optional dictionary of filter names to field names that the element listens to.

        https://cloud.google.com/looker/docs/reference/param-lookml-dashboard-table-chart#listen
        """
        return self.listen.listens if self.listen else None

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
                elif isinstance(attr_value, (LookMLListen, LookMLColorApplication)):
                    formatted_value = attr_value.build()
                elif isinstance(attr_value, List) and all(
                    isinstance(item, LookMLSort) for item in attr_value
                ):
                    formatted_value = (
                        "["
                        + ", ".join(list_item.build() for list_item in attr_value)
                        + "]"
                    )
                elif isinstance(attr_value, List):
                    formatted_value = "[" + ",\n      ".join(attr_value) + "]"
                else:
                    raise ValueError(
                        f"Unexpected value provided for attribute {attribute} in LookML Dashboard element {self.name}"
                    )
                formatted_values.append(f"{attribute}: {formatted_value}")

        element_contents = "\n    ".join(formatted_values)
        return "- " + element_contents


def build_dashboard_grid(elements: List[LookMLDashboardElement]) -> None:
    """
    Builds a grid of elements for a LookML dashboard by setting the element's row column values.
    Elements should not have any row or column values set before calling this function.
    If an element is missing a width or height, it will be set to the default values.
    """
    row, column = 0, 0
    for element in elements:
        if element.row is not None or element.col is not None:
            raise ValueError(
                f"Element [{element.name}] already has a row or column value set."
            )
        if element.width is None:
            element.width = SPLIT_SCREEN_WIDTH
        if element.height is None:
            element.height = DEFAULT_HEIGHT

        if column + element.width > FULL_SCREEN_WIDTH:
            row += element.height
            column = 0

        element.row = row
        element.col = column

        column += element.width
