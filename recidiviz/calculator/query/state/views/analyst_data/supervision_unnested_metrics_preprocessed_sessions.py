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
"""View tracking daily metrics at the officer-office level"""
from typing import List, Tuple

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_unnested_metrics import (
    SUPERVISION_METRICS_SUPPORTED_LEVELS,
    SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS,
    SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_RENAME = {
    "supervising_officer_external_id": "state_code, supervising_officer_external_id AS officer_id",
    "supervision_office": "state_code, supervision_district AS district, supervision_office AS office",
    "supervision_district": "state_code, supervision_district AS district",
    "state_code": "state_code",
}


def get_supervision_unnested_metrics_view_strings_by_level(
    level: str,
) -> Tuple[str, str, str]:
    """
    Takes as input the level of the metrics dataframe, i.e. one from
    SUPERVISION_METRICS_SUPPORTED_LEVELS.

    Returns a list with four strings:
    1. view_id
    2. view_description
    3. query_template
    """

    if level not in SUPERVISION_METRICS_SUPPORTED_LEVELS:
        raise ValueError(f"`level` must be in {SUPERVISION_METRICS_SUPPORTED_LEVELS}")

    level_name = SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES[level]
    view_id = f"supervision_{level_name}_unnested_metrics_preprocessed_sessions"
    index_cols_long = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_RENAME[level]
    index_cols = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[level]

    view_description = f"""Subquery that extracts appropriate rows from compartment_sessions
for use in the supervision_{level_name}_unnested_metrics table.
"""

    # get client-period source table
    if level == "supervising_officer_external_id":
        table = "supervision_officer_sessions_materialized"
    elif level == "state_code":
        table = "compartment_sessions_materialized"
    else:
        table = "location_sessions_materialized"
    client_period_table = f"{{project_id}}.{{sessions_dataset}}.{table}"

    query_template = f"""

WITH 
-- define supervision population
-- We only include supervised clients in designated compartment_level_2 for metrics.
-- This is for consistency across states when defining a supervision sample.
sample AS (
    SELECT
        {"state_code," if level_name == "state" else ""}
        person_id,
        start_date AS sample_start_date,
        -- TODO(#14675): remove the DATE_ADD when session end_dates are exclusive
        DATE_ADD(end_date, INTERVAL 1 DAY) AS sample_end_date,
    FROM
        `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
    WHERE
        -- require that clients be associated with a compartment that is part of the
        -- target "supervision" population for all aggregated metrics
        compartment_level_1 = "SUPERVISION"
        AND compartment_level_2 IN (
            "COMMUNITY_CONFINEMENT", "DUAL", "INFORMAL_PROBATION", "PAROLE", "PROBATION"
        )
)

-- client assignments to {level_name}
-- if client not always in sample population, take intersection of inclusive periods
-- to determine the start and end dates of assignment
, potentially_adjacent_spans AS (
    SELECT
{'''
        * EXCEPT(sample_start_date, sample_end_date),
        sample_start_date AS assignment_date,
        -- impute end date as 9999-01-01 if null (we'll adjust in the next cte)
        IFNULL(sample_end_date, "9999-01-01") AS end_date,
    FROM
        sample
''' if level_name == "state" else f'''
        {index_cols_long},
        assign.person_id,
        -- latest start date of overlap is the assignment date
        GREATEST(sample_start_date, start_date) AS assignment_date,
        -- earliest end date of overlap is the end of association.
        -- end_date here is exclusive, i.e. the date of transition, but leave as 
        -- 9999-01-01 if null (we'll adjust in the next cte)
        LEAST(
            IFNULL(end_date, "9999-01-01"),
            IFNULL(sample_end_date, "9999-01-01")
        ) AS end_date,
    FROM (
        SELECT
            * EXCEPT (end_date),
            -- TODO(#14675): remove the DATE_ADD when end_dates are exclusive upstream
            DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
        FROM
            `{client_period_table}`
    ) assign
    INNER JOIN
        sample 
    ON
        sample.person_id = assign.person_id
        -- sample and assignment spans must overlap
        AND (
            sample_start_date BETWEEN start_date AND IFNULL(end_date, "9999-01-01")
            OR start_date BETWEEN sample_start_date AND IFNULL(sample_end_date, 
                "9999-01-01")
        )
    WHERE
        {level} IS NOT NULL
'''}
)

-- now session-ize contiguous periods
, {level_name}_assignments AS (
    SELECT
        {index_cols},
        person_id,
        session_id,
        MIN(assignment_date) AS assignment_date,
        NULLIF(MAX(end_date), "9999-01-01") AS end_date,
    FROM (
        SELECT
            * EXCEPT(date_gap),
            SUM(IF(date_gap, 1, 0)) OVER (
                PARTITION BY {index_cols}, person_id ORDER BY assignment_date
            ) AS session_id,
        FROM (
            SELECT
                *,
                IFNULL(
                    LAG(end_date) OVER(
                        PARTITION BY {index_cols}, person_id ORDER BY assignment_date
                    ) != assignment_date, TRUE
                ) AS date_gap,
            FROM
                potentially_adjacent_spans
        )
    )
    GROUP BY {index_cols}, person_id, session_id
)
SELECT * FROM {level_name}_assignments;"""

    return view_id, view_description, query_template


# init object to hold view builders
SUPERVISION_UNNESTED_METRICS_PREPROCESSED_SESSIONS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = []

for level_string in SUPERVISION_METRICS_SUPPORTED_LEVELS:

    (
        view_id_string,
        view_description_string,
        query_template_string,
    ) = get_supervision_unnested_metrics_view_strings_by_level(level_string)

    clustering_fields = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[
        level_string
    ].split(", ")

    SUPERVISION_UNNESTED_METRICS_PREPROCESSED_SESSIONS_VIEW_BUILDERS.append(
        SimpleBigQueryViewBuilder(
            dataset_id=ANALYST_VIEWS_DATASET,
            view_id=view_id_string,
            view_query_template=query_template_string,
            description=view_description_string,
            sessions_dataset=SESSIONS_DATASET,
            clustering_fields=clustering_fields,
            should_materialize=True,
        )
    )

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for (
            view_builder
        ) in SUPERVISION_UNNESTED_METRICS_PREPROCESSED_SESSIONS_VIEW_BUILDERS:
            view_builder.build_and_print()
