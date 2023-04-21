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
"""
Helper SQL queries to find almost eligible individuals
"""
from typing import Optional


def json_to_array_cte(from_table: str) -> str:
    """Helper method that returns a CTE where the
    reasons JSON is transformed to an array for
    easier later manipulation. Returned CTE also called
    'json_to_array_cte'.

    Args:
        from_table (str): Table to query from
    """

    return f""" json_to_array_cte AS (
    -- Transform the reason json column into an array for easier manipulation
    SELECT 
        *,
        JSON_QUERY_ARRAY(reasons) AS array_reasons
    FROM {from_table}
    )
    """


def one_criteria_away_from_eligibility(
    criteria_name: str,
    criteria_condition: Optional[str] = None,
    field_name_in_reasons_blob: Optional[str] = None,
) -> str:
    """Helper method that returns a query where individuals
    with only one ineligible criteria (criteria_name) are
    returned.

    This method requires json_to_array_cte CTE to be defined previously in
    the query (see function with the same name above). It requires the columns
    ineligible_criteria, array_reasons, and is_eligible.

    Args:
        criteria_name (str): Name of criteria; has_usually the following form
            US_XX_CRITERIA_NAME
        criteria_condition (str): String with a condition to filter AE cases. E.g.
            if criteria_condition = '< 100', only clients with a 'criteria_name' of less
            than 100 will be surfaced as almost eligible.
        field_name_in_reasons_blob (str): Field name where the value is stored in the
        reasons column of the eligibility spans.
    """

    criteria_value_query_fragment = ""
    criteria_value_where_clause = ""

    # If a criteria_condition was specified, we create the string that will pull
    #   the value for us as a column (criteria_value_query_fragment) and we filter
    #   based off this column and the criteria_condition
    if criteria_condition is not None:
        criteria_value_query_fragment = f"""
        CAST( 
            ARRAY(
                SELECT JSON_VALUE(x.reason.{field_name_in_reasons_blob})
                FROM UNNEST(array_reasons) AS x
                WHERE STRING(x.criteria_name) = '{criteria_name}'
            )[OFFSET(0)] AS FLOAT64) AS criteria_value
        """

        criteria_value_where_clause = f"""
        WHERE criteria_value {criteria_condition}
        """

    return f"""
SELECT 
    external_id,
    state_code,
    reasons,
    ineligible_criteria
FROM (SELECT
        * EXCEPT(array_reasons, is_eligible),
        {criteria_value_query_fragment}
    FROM json_to_array_cte
    WHERE 
        -- keep if only ineligible criteria is criteria_name
        '{criteria_name}' IN UNNEST(ineligible_criteria) 
        AND ARRAY_LENGTH(ineligible_criteria) = 1
)
{criteria_value_where_clause}
"""


def x_time_away_from_eligibility(
    criteria_name: str,
    time_interval: int,
    date_part: str,
    eligible_date: str = "eligible_date",
) -> str:
    """Helper method that returns a query where individuals who are a time_interval
    (e.g. 4 months) away from eligibility are surfaced

    This method requires json_to_array_cte CTE to be defined previously in
    the query (see function with the same name above). It also requires it to
    contain the columns ineligible_criteria, array_reasons, and is_eligible

    Args:
        criteria_name (str): Name of criteria; has_usually the following form
            US_XX_CRITERIA_NAME
        time_interval (int): Integer that combines with date_part to represent to a time
            interval
        date_part (str): BigQuery date_part values. E.g. DAY, MONTH, YEAR, etc.
        eligible_date (str, optional): Name of column that represents the eligibility
            date. Defaults to "eligible_date".
    """

    return f"""SELECT * EXCEPT({eligible_date}, is_eligible)
FROM   (SELECT
            * EXCEPT(array_reasons),
            -- only keep {eligible_date} for the relevant criteria
            CAST(
                ARRAY(
                    SELECT JSON_VALUE(x.reason.{eligible_date})
                    FROM UNNEST(array_reasons) AS x
                    WHERE STRING(x.criteria_name) = '{criteria_name}'
                )[OFFSET(0)]
            AS DATE)  AS {eligible_date},
            FROM json_to_array_cte
        WHERE 
        -- keep if only ineligible criteria is time remaining on sentence
        '{criteria_name}' IN UNNEST(ineligible_criteria) 
        AND ARRAY_LENGTH(ineligible_criteria) = 1
        )
WHERE DATE_DIFF({eligible_date}, CURRENT_DATE('US/Pacific'), {date_part}) < {time_interval}"""
