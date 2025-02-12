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
"""View of continuous spans at a given client count for the specified assignment sessions"""

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_caseload_count_spans"

_VIEW_DESCRIPTION = (
    "Spans defined per officer based on periods of a continuous caseload size"
)

_QUERY_TEMPLATE = """
WITH assignment_spans AS (
    SELECT
        state_code,
        officer_id,
        person_id,
        assignment_date AS start_date,
        end_date
    FROM
        `{project_id}.{aggregated_metrics_dataset}.supervision_officer_metrics_person_assignment_sessions_materialized`
)
,
population_change_dates AS (
    -- Start dates increase population by 1
    SELECT
        state_code,
        officer_id,
        start_date AS change_date,
        1 AS change_value
    FROM assignment_spans
    UNION ALL
    -- End dates decrease population by 1
    SELECT
        state_code,
        officer_id,
        end_date AS change_date,
        -1 AS change_value
    FROM assignment_spans
    WHERE end_date IS NOT NULL
)
,
population_change_dates_agg AS (
    SELECT
        state_code, 
        officer_id, 
        change_date, 
        SUM(change_value) AS change_value
    FROM 
        population_change_dates
    GROUP BY 1, 2, 3
)
SELECT
    state_code,
    officer_id,
    change_date AS start_date,
    LEAD(change_date) OVER (PARTITION BY state_code, officer_id ORDER BY change_date) AS end_date,
    SUM(change_value) OVER (PARTITION BY state_code, officer_id ORDER BY change_date) AS caseload_count,
FROM 
    population_change_dates_agg
"""

SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=AGGREGATED_METRICS_DATASET_ID,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    aggregated_metrics_dataset=AGGREGATED_METRICS_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER.build_and_print()
