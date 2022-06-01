#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
"""Template for queries to count transitions out of supervision by month."""

from typing import List


def _get_zero_imputation_query(
    dimensions: List[str],
    dimension_combination_view: str,
) -> str:
    """Builds a query that calculates all combinations of the provided dimensions
    and imputes the event counts to zero where the aggregates are NULL."""

    return f"""
    SELECT
        * EXCEPT (event_count),
        IFNULL(event_count, 0) AS event_count,
    FROM
        aggregate_event_counts
    FULL OUTER JOIN
        (
            SELECT DISTINCT
                state_code,
                year,
                month,
                {','.join(dimensions)},
            FROM `{{project_id}}.{{dashboard_views_dataset}}.{dimension_combination_view}`
        )USING (state_code, year, month,{','.join(dimensions)})
    """


def transition_monthly_aggregate_template(
    aggregate_query: str,
    dimensions: List[str],
    dimension_combination_view: str,
) -> str:
    """Constructs the boilerplate parts of the metric view (building date arrays, missing rows).
    `aggregate_query` should have a field for each provided dimension as well as "state_code", "year", and "month"
    and compute a "event_count" field."""

    return f"""
    WITH aggregate_event_counts AS (
        {aggregate_query}
    ), 
    blanks_filled AS (
        {_get_zero_imputation_query(dimensions, dimension_combination_view)}
    )

    SELECT *
    FROM blanks_filled
    WHERE DATE(year, month, 1) BETWEEN
      DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 60 MONTH) AND CURRENT_DATE('US/Eastern')
    ORDER BY state_code, year, month, {', '.join([*dimensions])}
    """
