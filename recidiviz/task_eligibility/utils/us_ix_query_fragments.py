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
"""
Helper SQL queries for Idaho
"""
from typing import List
from recidiviz.calculator.query.bq_utils import nonnull_start_date_clause
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)

_DETAINER_TYPE_LST = ["23", "32"]


def date_within_time_span(
    meets_criteria_leading_window_days: int = 0, critical_date_column: str = ""
) -> str:
    """
    Generates a BigQuery SQL query that identifies time spans where a critical date
    falls within a specified leading window of days.

    Args:
        meets_criteria_leading_window_days (int, optional): The leading window of days
            used to determine if a critical date falls within. Default is 0.
        critical_date_column (str, optional): The column representing the critical date
            to be checked within the time spans. Default is an empty string.

    Returns:
        str: A formatted BigQuery SQL query that selects time spans and determines if the
        critical date has passed within the specified leading window.

    Example usage:
        query = date_within_time_span(meets_criteria_leading_window_days=5, critical_date_column="custom_date")
        # Execute the generated query using your preferred method
    """
    return f"""
    WITH
      critical_date_spans AS (
          SELECT
            state_code,
            person_id,
            {nonnull_start_date_clause('start_date')} as start_datetime,
            end_date as end_datetime,
            {critical_date_column} AS critical_date
          FROM
            `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized`
          WHERE {critical_date_column} IS NOT NULL AND start_date IS NOT NULL),
      {critical_date_has_passed_spans_cte(meets_criteria_leading_window_days)}
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      critical_date_has_passed AS meets_criteria,
      TO_JSON(STRUCT(critical_date AS {critical_date_column})) AS reason,
    FROM
      critical_date_has_passed_spans
    """


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
    return f"""
    WITH
      detainer_cte AS (
          SELECT
            *
          FROM
            `{{project_id}}.{{analyst_dataset}}.us_ix_detainer_spans_materialized`
          WHERE DetainerTypeId IN {reformatted_types_to_include_lst}),
          {create_sub_sessions_with_attributes('detainer_cte')}
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      False as meets_criteria,
      TO_JSON(STRUCT(start_date AS latest_detainer_start_date,
          DetainerTypeDesc AS latest_detainer_type,
          DetainerStatusDesc AS latest_detainer_status)) AS reason,
    FROM
      sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY start_date DESC)=1
    """