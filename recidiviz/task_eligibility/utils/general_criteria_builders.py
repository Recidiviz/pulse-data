# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper methods that return criteria view builders with similar logic that
can be parameterized.
"""
from typing import List, Optional

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)


def get_minimum_age_criteria(
    minimum_age: int,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state agnostic criteria view builder indicating the spans of time when a person is
    |minimum_age| years or older
    """
    criteria_name = f"AGE_{minimum_age}_YEARS_OR_OLDER"

    criteria_description = f"""Defines a criteria span view that shows spans of time during which someone
     is {minimum_age} years old or older"""

    criteria_query = f"""
    SELECT
        state_code,
        person_id,
        DATE_ADD(birthdate, INTERVAL {minimum_age} YEAR) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(
            DATE_ADD(birthdate, INTERVAL {minimum_age} YEAR) AS eligible_date
        )) AS reason,
    FROM `{{project_id}}.{{sessions_dataset}}.person_demographics_materialized`
    -- Drop any erroneous birthdate values
    WHERE birthdate <= CURRENT_DATE("US/Eastern")
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=criteria_description,
        criteria_spans_query_template=criteria_query,
        sessions_dataset=SESSIONS_DATASET,
    )


def get_minimum_time_served_criteria_query(
    criteria_name: str,
    description: str,
    minimum_time_served_years: int,
    compartment_level_1_types: Optional[List[str]] = None,
    compartment_level_2_types: Optional[List[str]] = None,
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state agnostic criteria view builder indicating spans of time when a person has served
    |minimum_time_served| years or more. The compartment level filters can be used to restrict the type of session
    that counts towards the time served."""

    # Default to `system_sessions` if no compartment type is specified
    sessions_table = "system_sessions_materialized"
    sessions_conditions = []

    if compartment_level_1_types:
        sessions_table = "compartment_level_1_super_sessions_materialized"
        sessions_conditions.append(
            f"compartment_level_1 IN ('{', '.join(compartment_level_1_types)}')"
        )

    if compartment_level_2_types:
        sessions_table = "compartment_sessions_materialized"
        sessions_conditions.append(
            f"compartment_level_2 IN ('{', '.join(compartment_level_2_types)}')"
        )

    if len(sessions_conditions) > 0:
        condition_string = "WHERE " + "\n\t\tAND ".join(sessions_conditions)
    else:
        condition_string = ""

    criteria_query = f"""
    WITH critical_date_spans AS (
      SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        DATE_ADD(start_date, INTERVAL {minimum_time_served_years} YEAR) AS critical_date,
      FROM `{{project_id}}.{{sessions_dataset}}.{sessions_table}`
      {condition_string}
    ),
    {critical_date_has_passed_spans_cte()}
    SELECT
        cd.state_code,
        cd.person_id,
        cd.start_date,
        cd.end_date,
        cd.critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            cd.critical_date AS eligible_date
        )) AS reason,
    FROM critical_date_has_passed_spans cd
    """

    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criteria_query,
        sessions_dataset=SESSIONS_DATASET,
    )
