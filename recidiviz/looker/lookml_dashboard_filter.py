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
"""Defines a class representing a filter as part of the `filters`
   parameter on a LookML Dashboard, which allows the user to change 
   what data is shown on the dashboard.

   Documentation:
   https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#filters
   """
from enum import Enum
from typing import Optional

import attr

from recidiviz.utils.string import StrictStringFormatter

UI_CONFIG_TEMPLATE = """
      type: {type}
      display: {display}"""


class LookMLFilterType(Enum):
    FIELD_FILTER = "field_filter"
    NUMBER_FILTER = "number_filter"
    DATE_FILTER = "date_filter"
    STRING_FILTER = "string_filter"


class LookMLFilterUIType(Enum):
    DROPDOWN_MENU = "dropdown_menu"
    ADVANCED = "advanced"
    TAG_LIST = "tag_list"


class LookMLFilterUIDisplay(Enum):
    INLINE = "inline"
    POPOVER = "popover"
    OVERFLOW = "overflow"


@attr.define
class LookMLFilterUIConfig:
    """
    Represents a `ui_config` parameter of a filter, including a `type` and `display`
    sub-parameters. Not all UI configurations are supported.
    Documentation: https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#ui_config
    """

    type: LookMLFilterUIType
    display: LookMLFilterUIDisplay

    def build(self) -> str:
        """
        Returns the string representation of this ui config.
        """
        return StrictStringFormatter().format(
            UI_CONFIG_TEMPLATE,
            type=self.type.value,
            display=self.display.value,
        )


@attr.define
class LookMLDashboardFilter:
    """Generates a `filter` parameter for a dashboard,
    which allow the user to change what data is shown on the dashboard.
    Documentation: https://cloud.google.com/looker/docs/reference/param-lookml-dashboard#filters
    """

    name: str
    title: Optional[str] = attr.field(default=None)
    type: Optional[LookMLFilterType] = attr.field(default=None)
    default_value: Optional[str] = attr.field(default=None)
    allow_multiple_values: Optional[bool] = attr.field(default=None)
    required: Optional[bool] = attr.field(default=None)
    ui_config: Optional[LookMLFilterUIConfig] = attr.field(default=None)
    model: Optional[str] = attr.field(default=None)
    explore: Optional[str] = attr.field(default=None)
    field: Optional[str] = attr.field(default=None)

    def __attrs_post_init__(self) -> None:
        if self.field and "." not in self.field:
            raise ValueError(
                f"LookML filter {self.name} has the field {self.field}, but field names"
                f"should be fully scoped: use view_name.field_name, not just field_name"
            )

    def build(self) -> str:
        """
        Return a formatted string representing the filter itself.
        """
        formatted_values = []
        for attribute in attr.fields_dict(self.__class__):
            attr_value = getattr(self, attribute)
            # skip attributes that were not provided
            if attr_value is not None:
                if isinstance(attr_value, str):
                    formatted_value = attr_value
                elif isinstance(attr_value, bool):
                    formatted_value = str(attr_value).lower()
                elif isinstance(attr_value, Enum):
                    formatted_value = attr_value.value
                elif isinstance(attr_value, LookMLFilterUIConfig):
                    formatted_value = attr_value.build()
                else:
                    raise ValueError(
                        f"Unexpected value provided for attribute {attribute} in LookML Dashboard filter {self.name}"
                    )
                formatted_values.append(f"{attribute}: {formatted_value}")

        filter_contents = "\n    ".join(formatted_values)
        return "- " + filter_contents
