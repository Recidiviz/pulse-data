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
"""Creates the view builder and view for person attributes at the time of variant
assignment for those in an experiment."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_ATTRIBUTES_VIEW_NAME = "person_attributes"

PERSON_ATTRIBUTES_VIEW_DESCRIPTION = (
    "Calculates person-level attributes for those assigned a variant in the "
    "assignments table. All attributes are specific to person-experiment-variant, so "
    "there may be multiple observations per person and multiple observations per "
    "person-experiment. All attributes are calculated at time of variant assignment."
)

PERSON_ATTRIBUTES_PRIMARY_KEYS = "experiment_id, state_code, person_id, variant_date"

PERSON_ATTRIBUTES_QUERY_TEMPLATE = """
WITH participants AS (
    SELECT DISTINCT
        {primary_keys},
    FROM
        `{project_id}.{experiments_dataset}.person_assignments_materialized`
)

# district of each client pre assignment, looking back up to one month prior to 
# assignment, then up to one month following.
, person_id_districts AS (
    SELECT DISTINCT
        experiment_id,
        a.state_code,
        a.person_id,
        variant_date,
        "DISTRICT" AS attribute,
        ARRAY_AGG(
            IFNULL(supervising_district_external_id, "INTERNAL_UNKNOWN") 
            -- priority to pre-treatment district, then closest to variant date
            ORDER BY
                date_of_supervision <= variant_date,
                ABS(DATE_DIFF(variant_date, date_of_supervision, DAY))
            ASC LIMIT 1)[OFFSET(0)] AS value,
    FROM
        participants a
    LEFT JOIN
        `{project_id}.{dataflow_dataset}.most_recent_supervision_population_span_to_single_day_metrics_materialized` b
    ON
        a.state_code = b.state_code
        AND a.person_id = b.person_id
        AND b.date_of_supervision BETWEEN DATE_SUB(a.variant_date, INTERVAL 30 DAY) AND 
            DATE_ADD(a.variant_date, INTERVAL 30 DAY)
        AND b.included_in_state_population
    GROUP BY 1, 2, 3, 4, 5
)

# type of compartment, for now just incarceration, parole, probation, dual, or other
, person_id_compartments AS (
    SELECT
        experiment_id,
        a.state_code,
        a.person_id,
        variant_date,
        "COMPARTMENT" AS attribute,
        CASE
            WHEN compartment_level_1 IN ("INCARCERATION") THEN compartment_level_1
            WHEN compartment_level_2 IN ("PAROLE", "PROBATION", "DUAL") THEN compartment_level_2
            ELSE "OTHER" END AS value,
    FROM
        participants a
    LEFT JOIN
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` b
    ON
        a.state_code = b.state_code
        AND a.person_id = b.person_id
        AND a.variant_date BETWEEN b.start_date AND 
            IFNULL(b.end_date, "9999-01-01")
)

# wide demographics table for person_id
, person_id_demographics_wide AS (
    SELECT DISTINCT
        experiment_id,
        a.state_code,
        a.person_id,
        variant_date,
        CAST(IF(assessment_type = "LSIR", assessment_score, NULL) AS STRING) AS LSIR_SCORE,
        CAST(DATE_DIFF(variant_date, assessment_date, DAY) AS STRING) AS LSIR_DAYS_SINCE,
    FROM
        participants a
    LEFT JOIN
        `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` c
    ON
        a.state_code = c.state_code
        AND a.person_id = c.person_id
        AND a.variant_date BETWEEN c.assessment_date AND 
            IFNULL(c.score_end_date, "9999-01-01")
)
    
# wide to long for person_id demographics
SELECT
    {primary_keys},
    attribute,
    value,
FROM
    person_id_demographics_wide
UNPIVOT (
    value FOR attribute IN (
        LSIR_SCORE, LSIR_DAYS_SINCE
    )
)

# union CTEs in long format

UNION ALL
SELECT * FROM person_id_districts
UNION ALL
SELECT * FROM person_id_compartments
"""

PERSON_ATTRIBUTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=PERSON_ATTRIBUTES_VIEW_NAME,
    view_query_template=PERSON_ATTRIBUTES_QUERY_TEMPLATE,
    description=PERSON_ATTRIBUTES_VIEW_DESCRIPTION,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    primary_keys=PERSON_ATTRIBUTES_PRIMARY_KEYS,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id", "attribute"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_ATTRIBUTES_VIEW_BUILDER.build_and_print()
