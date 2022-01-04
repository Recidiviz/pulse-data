# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates the view builder and view for outcome metrics for those in an active
experiment."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (  # STATIC_REFERENCE_TABLES_DATASET,
    ANALYST_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OUTCOMES_VIEW_NAME = "outcomes"

OUTCOMES_VIEW_DESCRIPTION = (
    "Calculates outcome metrics for those assigned a variant in the assignments table. "
    "All metrics are specific to person-experiment-variant, so may have multiple "
    "observations per person and multiple observations per person-experiment."
)

OUTCOME_METRIC_NAMES = [
    "INCARCERATION_START_FIRST",
    "INCARCERATION_START_COUNT_1_YEAR",
    "INCARCERATION_START_DAYS_TO_EVENT_1_YEAR",
    "LSIR_ASSESSMENT_COUNT_1_YEAR",
    "LSIR_ASSESSMENT_FIRST",
    "VIOLATION_RESPONSE_COUNT_1_YEAR",
    "VIOLATION_RESPONSE_FIRST",
]

OUTCOMES_QUERY_TEMPLATE = """
WITH participants AS (
    SELECT DISTINCT
        a.experiment_id,
        a.state_code,
        a.subject_id,
        a.id_type,
        variant_date,
    FROM
        `{project_id}.{experiments_dataset}.assignments_materialized` a
    -- add experiment end date
),

participants_metrics_menu AS (
    SELECT * 
    FROM
        participants,
        UNNEST({metrics}) AS metric
)

-- First time event occurred, otherwise current date and zero
SELECT
    a.experiment_id,
    a.state_code, 
    a.subject_id,
    a.id_type,
    a.variant_date,
    a.metric AS metric,
    COALESCE(b.event_date, CURRENT_DATE('US/Eastern')) AS date,
    IF(b.event_date IS NOT NULL, 1, 0) AS value,
FROM
    participants_metrics_menu a
LEFT JOIN
    `{project_id}.{analyst_views_dataset}.person_events_materialized` b
ON
    a.state_code = b.state_code
    AND CAST(a.subject_id AS INT64) = b.person_id
    AND b.event_date BETWEEN a.variant_date AND CURRENT_DATE('US/Eastern')  -- swap for experiment end date
    AND a.metric = CONCAT(b.event, "_FIRST")
WHERE
    a.metric LIKE ("%_FIRST")
    AND a.id_type = "person_id"
QUALIFY
    -- keep first occurrence of event, if any
    ROW_NUMBER() OVER (
        PARTITION BY a.experiment_id, a.state_code, a.subject_id, a.id_type, 
            a.variant_date, a.metric
        ORDER BY COALESCE(b.event_date, "9999-01-01")
    ) = 1

UNION ALL 

-- Number of events that occurred in first year
-- Should we turn this into months and parameterize the number of months?
SELECT DISTINCT
    a.experiment_id,
    a.state_code, 
    a.subject_id,
    a.id_type,
    a.variant_date,
    a.metric,
    DATE_ADD(a.variant_date, INTERVAL 1 YEAR) AS date,
    -- Null metric if full year hasn't yet passed
    IF(DATE_ADD(a.variant_date, INTERVAL 1 YEAR) < CURRENT_DATE('US/Eastern'),
        COUNTIF(b.event_date IS NOT NULL), NULL) AS value,
FROM
    participants_metrics_menu a
LEFT JOIN
    `{project_id}.{analyst_views_dataset}.person_events_materialized` b
ON
    a.state_code = b.state_code
    AND CAST(a.subject_id AS INT64) = b.person_id
    AND b.event_date BETWEEN a.variant_date AND 
        DATE_ADD(a.variant_date, INTERVAL 1 YEAR)
    AND a.metric = CONCAT(b.event, "_COUNT_1_YEAR")
WHERE
    a.metric LIKE("%_COUNT_1_YEAR")
    AND a.id_type = "person_id"
GROUP BY 1, 2, 3, 4, 5, 6

UNION ALL 

-- Days to first event in 1 year
-- Should we turn parameterize the number of capped months?
SELECT
    a.experiment_id,
    a.state_code, 
    a.subject_id,
    a.id_type,
    a.variant_date,
    a.metric AS metric,
    DATE_ADD(a.variant_date, INTERVAL 1 YEAR) AS date,
    -- Null metric if full year hasn't yet passed
    IF(DATE_ADD(a.variant_date, INTERVAL 1 YEAR) < CURRENT_DATE('US/Eastern'),
        IF(DATE_ADD(a.variant_date, INTERVAL 1 YEAR) >= b.event_date, 
            DATE_DIFF(b.event_date, a.variant_date, DAY),
            365
        ),
        NULL
    ) AS value,
FROM
    participants_metrics_menu a
LEFT JOIN
    `{project_id}.{analyst_views_dataset}.person_events_materialized` b
ON
    a.state_code = b.state_code
    AND CAST(a.subject_id AS INT64) = b.person_id
    AND b.event_date BETWEEN a.variant_date AND 
        DATE_ADD(a.variant_date, INTERVAL 1 YEAR)
    AND a.metric = CONCAT(b.event, "_DAYS_TO_EVENT_1_YEAR")
WHERE
    a.metric LIKE("%_DAYS_TO_EVENT_1_YEAR")
    AND a.id_type = "person_id"
QUALIFY
    -- keep first occurrence of event, if any
    ROW_NUMBER() OVER (
        PARTITION BY a.experiment_id, a.state_code, a.subject_id, a.id_type, 
            a.variant_date, a.metric
        ORDER BY COALESCE(b.event_date, "9999-01-01")
    ) = 1

"""

OUTCOMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=OUTCOMES_VIEW_NAME,
    view_query_template=OUTCOMES_QUERY_TEMPLATE,
    description=OUTCOMES_VIEW_DESCRIPTION,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    # TODO(#9986): use experiment start and end dates to bound outcome ranges
    # static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id", "metric"],
    metrics=str(OUTCOME_METRIC_NAMES),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OUTCOMES_VIEW_BUILDER.build_and_print()
