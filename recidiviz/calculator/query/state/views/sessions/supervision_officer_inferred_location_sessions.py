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
"""Sessionized view of supervision officers. Session defined as continuous time
associated with a given primary office with at least a critical caseload count."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.dataflow_sessions import (
    DATAFLOW_SESSIONS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_inferred_location_sessions"

_VIEW_DESCRIPTION = (
    "Sessionized view of officers' inferred locations. Session defined as continuous stay "
    "associated with a given office + district. This association is determined as the "
    "modal location across each officer's caseload. end_date is exclusive."
)

_QUERY_TEMPLATE = f"""
WITH dataflow_sessions AS (
    SELECT DISTINCT
        state_code,
        person_id,
        attr.supervising_officer_external_id,
        attr.supervision_district,
        attr.supervision_district_name,
        attr.supervision_office,
        attr.supervision_office_name,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.{{dataflow_sessions}}`,
        UNNEST(session_attributes) AS attr
    WHERE
        compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
        AND attr.supervising_officer_external_id IS NOT NULL
)
    
, population_change_dates AS (
    -- Start dates increase population by 1
    SELECT
        state_code,
        supervising_officer_external_id,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        start_date AS change_date,
        1 AS change_value,
    FROM
        dataflow_sessions
    
    UNION ALL
    
    -- End dates decrease population by 1
    SELECT
        state_code,
        supervising_officer_external_id,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        end_date_exclusive AS change_date,
        -1 AS change_value,
    FROM
        dataflow_sessions
    WHERE
        end_date_exclusive IS NOT NULL
)

, population_change_dates_agg AS (
    SELECT
        state_code,
        supervising_officer_external_id,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        change_date,
        SUM(change_value) AS change_value,
    FROM 
        population_change_dates
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

, officer_office_caseload_counts AS (
    SELECT
        state_code,
        supervising_officer_external_id,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        change_date AS start_date,
        LEAD(change_date) OVER w AS end_date_exclusive,
        SUM(change_value) OVER w AS caseload_count,
    FROM 
        population_change_dates_agg
    WINDOW w AS (
        PARTITION BY state_code, supervising_officer_external_id,
            supervision_office, supervision_district
        ORDER BY change_date ASC
    )
)

/*
At this point we have caseload spans for officer-offices.
Strategy: at each change date there may be overlapping spans. At each change date,
choose the office with the greatest caseload count.
*/
, primary_offices AS (
    SELECT
        a.state_code,
        a.supervising_officer_external_id,
        change_date,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        caseload_count > 0 AS nonzero_caseload,
    FROM (
        -- each change date is a potential place for a new primary office span
        SELECT DISTINCT
            state_code,
            supervising_officer_external_id,
            change_date,
        FROM
            population_change_dates
    ) a
    INNER JOIN 
        officer_office_caseload_counts b
    ON
        a.state_code = b.state_code
        AND a.supervising_officer_external_id = b.supervising_officer_external_id
        AND a.change_date BETWEEN b.start_date AND
            {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
    QUALIFY
        -- keep non-null office with greatest caseload count, tiebreak by district
        -- then office name alphabetically
        ROW_NUMBER() OVER (
            PARTITION BY a.state_code, a.supervising_officer_external_id, change_date
            ORDER BY
                IF(supervision_district IS NULL, 1, 0) ASC, -- give priority to non-null districts
                IF(supervision_district_name IS NULL, 1, 0) ASC, -- give priority to non-null district names
                IF(supervision_office IS NULL, 1, 0) ASC, -- give priority to non-null offices
                IF(supervision_office_name IS NULL, 1, 0) ASC, -- give priority to non-null office names
                caseload_count DESC, supervision_district ASC, supervision_district_name ASC, supervision_office ASC
        ) = 1
)

-- now turn primary office-transition_days to spans
, non_overlapping_primary_office_spans AS (
    SELECT
        state_code,
        supervising_officer_external_id,
        change_date AS start_date,
        LEAD(change_date) OVER (
            PARTITION BY state_code, supervising_officer_external_id ORDER BY change_date ASC
        ) AS end_date_exclusive,
        supervision_office AS primary_office,
        supervision_office_name AS primary_office_name,
        supervision_district AS primary_district,
        supervision_district_name AS primary_district_name,
        nonzero_caseload,
    FROM
        primary_offices
)

-- remove zero caseload periods
-- this comes after the previous CTE so we can get end dates for the final caseload period
, zeros_removed AS (
    SELECT
        * EXCEPT(nonzero_caseload),
    FROM
        non_overlapping_primary_office_spans
    WHERE
        nonzero_caseload
)

-- now re-sessionize adjacent spans and return
, sessionized_cte AS (
{aggregate_adjacent_spans(
    table_name="zeros_removed",
    index_columns=["state_code", "supervising_officer_external_id"],
    attribute=["primary_office", "primary_office_name", "primary_district", "primary_district_name"],
    session_id_output_name="session_id",
    end_date_field_name="end_date_exclusive",
)})

SELECT
    state_code,
    supervising_officer_external_id,
    session_id,
    start_date,
    end_date_exclusive,
    primary_office,
    primary_office_name,
    primary_district,
    primary_district_name,
FROM
    sessionized_cte
"""

SUPERVISION_OFFICER_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    dataflow_sessions=DATAFLOW_SESSIONS_VIEW_BUILDER.table_for_query.to_str(),
    clustering_fields=["state_code", "supervising_officer_external_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
