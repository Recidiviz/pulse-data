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
"""Utils functions for common query logic used for span and event query builders"""

from typing import List


def package_json_attributes(attribute_cols: List[str]) -> str:
    """
    Returns a query fragment that casts attribute columns and combines into JSON blob.
    """
    attribute_cols_str_with_cast = ",\n".join(
        [f"        CAST({col} AS STRING) AS {col}" for col in attribute_cols]
    )
    return f"""TO_JSON_STRING(STRUCT(
{attribute_cols_str_with_cast}
    ))"""
