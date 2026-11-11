# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Detainer type lists and helper SQL queries for Idaho detainer spans."""

from typing import List

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)

DETAINER_TYPE_LST_CRC = ["59"]
HOLD_TYPE_LST_CRC = ["84", "112", "85", "61", "87", "86", "62", "88", "67", "83", "113"]
# Detainer types that IDOC counts when placing riders in a facility. Misdemeanor and
# federal holds don't count. ("F" is for "Felony", not "Federal".)
DETAINER_TYPE_LST_MOVES_MANAGEMENT = [
    "59",  # Detainer
    "67",  # Hold - Local County
    "83",  # Rider: Hold - Local
    "85",  # Hold: Local (F)
    "87",  # Hold: Untried (F)
    "112",  # Hold: Tried (F)
]
# detainer types related to felonies and immigration (https://docs.google.com/spreadsheets/d/1sV6BLWQQiQ4mrtzWJiooaHzO_sgk-bNkCMdwolQ8aVk/edit?usp=sharing)
DETAINER_TYPE_LST_CLD = [
    "2",
    "59",
    "60",
    "76",
    "78",
    "84",
    "85",
    "87",
    "98",
    "99",
    "101",
    "102",
    "108",
    "112",
    "115",
    "117",
    "121",
]


def detainer_span(types_to_include_lst: List[str]) -> str:
    """
    Retrieves detainer spans information based on the specified detainer type IDs.

    Args:
        types_to_include_lst (List[str]): A list of detainer type IDs to include in the query.

    Returns:
        str: SQL query string for retrieving detainer spans with the specified detainer types.

    Example:
        types_to_include = ['73', '23']
        query = detainer_span(types_to_include)
    """
    reformatted_types_to_include_lst = "('" + "', '".join(types_to_include_lst) + "')"
    return f"""WITH
    detainer_cte AS (
        SELECT *
        FROM `{{project_id}}.{{analyst_dataset}}.us_ix_detainer_spans_materialized`
        WHERE DetainerTypeId IN {reformatted_types_to_include_lst}),
          {create_sub_sessions_with_attributes('detainer_cte')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    TRUE as meets_criteria,
    TO_JSON(STRUCT(start_date AS latest_detainer_start_date,
        DetainerTypeDesc AS latest_detainer_type,
        DetainerStatusDesc AS latest_detainer_status)) AS reason,
    start_date AS latest_detainer_start_date,
    DetainerTypeDesc AS latest_detainer_type,
    DetainerStatusDesc AS latest_detainer_status,
FROM
    sub_sessions_with_attributes
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY start_date DESC)=1
    """
