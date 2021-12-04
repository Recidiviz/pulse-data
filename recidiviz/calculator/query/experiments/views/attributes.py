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
"""Creates the view builder and view for attributes at the time of variant assignment
for those in an experiment."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    PO_REPORT_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ATTRIBUTES_VIEW_NAME = "attributes"

ATTRIBUTES_VIEW_DESCRIPTION = (
    "Calculates subject-level attributes for those assigned a variant in the "
    "assignments table. All attributes are specific to person-experiment-variant, so "
    "there may be multiple observations per person and multiple observations per "
    "person-experiment. All attributes are calculated at time of variant assignment."
)

ATTRIBUTES_PRIMARY_KEYS = "experiment_id, state_code, subject_id, id_type, variant_date"

ATTRIBUTES_QUERY_TEMPLATE = """
WITH participants AS (
    SELECT DISTINCT
        {primary_keys},
    FROM
        `{project_id}.{experiments_dataset}.assignments_materialized`
)

# district of each client pre experiment, looking back up to one month prior to 
# experiment, then up to one month following experiment.
, person_id_districts AS (
    SELECT DISTINCT
        experiment_id,
        a.state_code,
        subject_id,
        id_type,
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
        `{project_id}.{dataflow_dataset}.most_recent_supervision_population_metrics_materialized` b
    ON
        a.state_code = b.state_code
        AND CAST(a.subject_id AS INT64) = b.person_id
        AND b.date_of_supervision BETWEEN DATE_SUB(a.variant_date, INTERVAL 30 DAY) AND 
            DATE_ADD(a.variant_date, INTERVAL 30 DAY)
    WHERE
        id_type = "person_id"
    GROUP BY 1, 2, 3, 4, 5, 6
)

# PO district at time of variant assignment
, po_districts AS (
    SELECT DISTINCT
        experiment_id,
        a.state_code,
        subject_id,
        id_type,
        variant_date,
        "DISTRICT" AS attribute,
        IFNULL(district, "INTERNAL_UNKNOWN") AS value,
    FROM
        participants a
    LEFT JOIN
        `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` b
    ON
        a.state_code = b.state_code
        AND a.subject_id = b.officer_external_id
        AND EXTRACT(MONTH from a.variant_date) = b.month
        AND EXTRACT(YEAR from a.variant_date) = b.year
    WHERE
        id_type = "officer_external_id"
)

# type of compartment, for now just incarceration, parole, probation, dual, or other
, person_id_compartments AS (
    SELECT
        experiment_id,
        a.state_code,
        subject_id,
        id_type,
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
        AND CAST(a.subject_id AS INT64) = b.person_id
        AND a.variant_date BETWEEN b.start_date AND 
            IFNULL(b.end_date, "9999-01-01")
    WHERE
        id_type = "person_id"
)

# wide demographics table for person_id
, person_id_demographics_wide AS (
    SELECT DISTINCT
        experiment_id,
        a.state_code,
        subject_id,
        id_type,
        variant_date,
        CAST(ROUND(DATE_DIFF(variant_date, birthdate, DAY)/365.25, 2) AS STRING) AS AGE,
        b.gender AS GENDER,
        b.prioritized_race_or_ethnicity AS PRIORITIZED_RACE,
        CAST(IF(assessment_type = "LSIR", assessment_score, NULL) AS STRING) AS LSIR_SCORE,
        CAST(DATE_DIFF(variant_date, assessment_date, DAY) AS STRING) AS LSIR_DAYS_SINCE,
    FROM
        participants a
    LEFT JOIN
        `{project_id}.{sessions_dataset}.person_demographics_materialized` b
    ON
        a.state_code = b.state_code
        AND CAST(a.subject_id AS INT64) = b.person_id
    LEFT JOIN
        `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` c
    ON
        a.state_code = c.state_code
        AND CAST(a.subject_id AS INT64) = c.person_id
        AND a.variant_date BETWEEN c.assessment_date AND 
            IFNULL(c.score_end_date, "9999-01-01")
    WHERE
        id_type = "person_id"
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
        AGE, GENDER, PRIORITIZED_RACE, LSIR_SCORE, LSIR_DAYS_SINCE
    )
)

# union CTEs in long format

UNION ALL
SELECT * FROM person_id_districts
UNION ALL
SELECT * FROM po_districts
UNION ALL
SELECT * FROM person_id_compartments
"""

ATTRIBUTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=ATTRIBUTES_VIEW_NAME,
    view_query_template=ATTRIBUTES_QUERY_TEMPLATE,
    description=ATTRIBUTES_VIEW_DESCRIPTION,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    po_report_dataset=PO_REPORT_DATASET,
    primary_keys=ATTRIBUTES_PRIMARY_KEYS,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id", "attribute"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ATTRIBUTES_VIEW_BUILDER.build_and_print()
