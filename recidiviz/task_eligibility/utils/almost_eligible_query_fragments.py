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
Helper SQL queries to find almost eligible individuals
"""
from typing import Optional


# TODO(#21350): Deprecate this file once all opportunities are migrated to almost eligible in TES
def json_to_array_cte(
    from_table: str,
) -> str:
    """Helper method that returns a statement where the
    reasons JSON is transformed to an array for
    easier later manipulation.

    Args:
        from_table (str): Table to query from
    """

    return f"""-- Transform the reason json column into an array for easier manipulation
    SELECT 
        *,
        JSON_QUERY_ARRAY(reasons) AS array_reasons
    FROM {from_table}
    """


def one_criteria_away_from_eligibility(
    criteria_name: str,
    criteria_condition: Optional[str] = None,
    field_name_in_reasons_blob: Optional[str] = None,
    from_cte_table_name: str = "json_to_array_cte",
    nested: bool = False,
    criteria_name_nested: Optional[str] = None,
) -> str:
    """Helper method that returns a query where individuals
    with only one ineligible criteria (criteria_name) are
    returned.

    This method requires that {from_cte_table_name} contains the following columns:
    ineligible_criteria, array_reasons (see the json_to_array_cte function
    above to get this column), and is_eligible.

    Args:
        criteria_name (str): Name of criteria; has_usually the following form
            US_XX_CRITERIA_NAME
        criteria_condition (str): String with a condition to filter AE cases. E.g.
            if criteria_condition = '< 100', only clients with a 'criteria_name' of less
            than 100 will be surfaced as almost eligible.
        field_name_in_reasons_blob (str): Field name where the value is stored in the
            reasons column of the eligibility spans.
        from_cte_table_name (str): Name of the CTE that contains the eligibility spans
        nested (bool): If true, it means the criteria being referenced has a nested "sub-criteria"
            that needs to be access
        criteria_name_nested (str): If nested is True, the name of the nested sub-criteria needs to be provided
    """

    criteria_value_query_fragment = ""
    criteria_value_where_clause = ""
    criteria_value_except_clause = ""

    # If a criteria_condition was specified, we create the string that will pull
    #   the value for us as a column (criteria_value_query_fragment) and we filter
    #   based off this column and the criteria_condition
    if criteria_condition is not None:
        if nested:
            criteria_value_query_fragment_inside = f"""
                JSON_QUERY_ARRAY((
                    SELECT x.reason
                    FROM UNNEST(array_reasons) AS x
                    WHERE STRING(x.criteria_name) = '{criteria_name}'
                  ))
            """

            criteria_value_query_fragment = f"""
                 CAST( 
                    ARRAY(
                        SELECT JSON_VALUE(x.reason.{field_name_in_reasons_blob})
                        FROM UNNEST({criteria_value_query_fragment_inside}) AS x
                        WHERE STRING(x.criteria_name) = '{criteria_name_nested}' 
                    )[OFFSET(0)] AS FLOAT64) AS criteria_value
            """
        else:
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

        criteria_value_except_clause = """
        EXCEPT(criteria_value, is_almost_eligible)
        """

    return f"""    SELECT
            * {criteria_value_except_clause}
        FROM (SELECT
                * EXCEPT(array_reasons, is_almost_eligible),
                {criteria_value_query_fragment}
            FROM {from_cte_table_name}
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
    from_cte_table_name: str = "json_to_array_cte",
) -> str:
    """Helper method that returns a query where individuals who are a time_interval
    (e.g. 4 months) away from eligibility are surfaced

    This method requires that {from_cte_table_name} contains the following columns:
    ineligible_criteria, array_reasons (see the json_to_array_cte function
    above to get this column), and is_eligible.

    Args:
        criteria_name (str): Name of criteria; has_usually the following form
            US_XX_CRITERIA_NAME
        time_interval (int): Integer that combines with date_part to represent to a time
            interval
        date_part (str): BigQuery date_part values. E.g. DAY, MONTH, YEAR, etc.
        eligible_date (str, optional): Name of column that represents the eligibility
            date. Defaults to "eligible_date".
        from_cte_table_name (str): Name of the CTE that contains the eligibility spans
    """

    return f"""SELECT * EXCEPT({eligible_date}, is_almost_eligible)
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
                FROM {from_cte_table_name}
            WHERE 
            -- keep if only ineligible criteria is time remaining on sentence
            '{criteria_name}' IN UNNEST(ineligible_criteria) 
            AND ARRAY_LENGTH(ineligible_criteria) = 1
            )
    WHERE DATE_DIFF({eligible_date}, CURRENT_DATE('US/Pacific'), {date_part}) < {time_interval}"""


def clients_eligible(from_cte: str) -> str:
    """Returns a SELECT statement that outputs all the currently eligible population.

    Args:
        from_cte (str): View/CTE to use after the FROM clause. Usually the one
            containing all the current eligibility spans of the relevant population

    """
    return f"""    SELECT * EXCEPT(is_almost_eligible)
        FROM {from_cte}
        WHERE is_eligible"""


def clients_eligible_and_almost_eligible(from_cte: str) -> str:
    """Returns a SELECT statement that outputs all the currently eligible or almost eligible population.

    Args:
        from_cte (str): View/CTE to use after the FROM clause. Usually the one
            containing all the current eligibility spans of the relevant population

    """
    return f"""    SELECT *
        FROM {from_cte}
        WHERE is_eligible OR is_almost_eligible"""
