# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""attr validators for big-query-specific requirements"""

from typing import Any

import attr

from recidiviz.common.google_cloud.big_query_utils import format_label_for_big_query


def is_valid_bq_label_value(
    _instance: Any, attribute: attr.Attribute, value: str
) -> None:
    if not isinstance(value, str):
        raise TypeError(
            f"Expected [{attribute.name}] to be a string, found [{type(value)}]"
        )
    if (formatted_label := format_label_for_big_query(value)) != value:
        raise TypeError(
            f"[{attribute.name}] is not a valid big query label, found [{value}]. "
            f"Please change to: [{formatted_label}]"
        )


def is_valid_bq_label_key(
    _instance: Any, attribute: attr.Attribute, value: str
) -> None:
    is_valid_bq_label_value(_instance, attribute, value)
    if len(value) == 0:
        raise TypeError(f"Expected [{attribute.name}] to be at least 1 character.")
