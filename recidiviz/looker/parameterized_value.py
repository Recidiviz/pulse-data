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
"""Creates ParameterizedValue class and associated functions for use in LookML"""

from typing import Callable, List

import attr


@attr.define
class ParameterizedValue:
    """
    A class that outputs a Liquid template of the form:

        {% if parameter_name._parameter_value == 'key1' %} value1
        {% elsif parameter_name._parameter_value == 'key2' %} value2
        ...
        {% endif %}

    for all of the keys and values in the provided dictionary.

    All lines after the first are indented by a number of spaces equal to
    twice the provided indentation level.
    """

    parameter_name: str
    parameter_options: List[str]
    value_builder: Callable[[str], str]
    indentation_level: int

    def __attrs_post_init__(self) -> None:
        if not self.parameter_name:
            raise ValueError("Parameter name must not be empty.")

        if not self.parameter_options:
            raise ValueError("Parameter options must not be empty.")

        if self.indentation_level < 0:
            raise ValueError("Indentation level must be non-negative.")

    def build_liquid_template(self) -> str:
        """
        Return the Liquid template corresponding to this parameterized value
        """
        indentation = "\n" + "  " * self.indentation_level
        liquid_template = ""
        for i, option in enumerate(self.parameter_options):
            boolean_clause = f"{self.parameter_name}._parameter_value == '{option}'"
            if i == 0:
                liquid_template += f"{{% if {boolean_clause} %}}"
            else:
                liquid_template += f"{indentation}{{% elsif {boolean_clause} %}}"
            liquid_template += f" {self.value_builder(option)}"
        liquid_template += f"{indentation}{{% endif %}}"
        return liquid_template
