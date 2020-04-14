# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Supervision Matrix by Person."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

SUPERVISION_MATRIX_BY_PERSON_VIEW_NAME = 'supervision_matrix_by_person'

SUPERVISION_MATRIX_BY_PERSON_DESCRIPTION = """
 Supervision matrix of violation response count and most severe violation per person.
 This lists all individuals on probation/parole/dual supervision, broken down by number of
 violations and the most severe violation while on supervision.
 """

SUPERVISION_MATRIX_BY_PERSON_QUERY = \
    """
    /*{description}*/
    WITH supervision_matrix AS (
        SELECT
            state_code, year, month, metric_period_months,
            CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN
                CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
                     WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
                     ELSE most_severe_violation_type END
                WHEN most_severe_violation_type IS NULL THEN 'NO_VIOLATIONS'
                ELSE most_severe_violation_type
            END AS violation_type,
            IF(response_count > 8, 8, response_count) as reported_violations,
            person_id, person_external_id,
            gender,
            IFNULL(assessment_score_bucket, 'OVERALL') AS risk_level,
            age_bucket,
            race, ethnicity,
            IFNULL(supervision_type, 'ALL') AS supervision_type,
            IFNULL(case_type, 'ALL') AS charge_category,
            IFNULL(supervising_district_external_id, 'ALL') as district,
            supervising_officer_external_id AS officer
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
        JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
            AND month IS NOT NULL
            AND person_id IS NOT NULL
            AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
            AND job.metric_type = 'SUPERVISION_POPULATION'
    ),
    dimension_explosion AS (
        SELECT
            state_code, year, month, metric_period_months,
            violation_type, reported_violations,
            supervision_type, charge_category,
            district, officer,
            person_id, person_external_id,
            gender, risk_level, age_bucket,
            race, ethnicity
        FROM supervision_matrix,
            UNNEST (['ALL', supervision_type]) supervision_type,
            UNNEST (['ALL', charge_category]) charge_category,
            UNNEST (['ALL', district]) district
    )
    SELECT
        state_code, year, month, metric_period_months, 
        violation_type, reported_violations,
        supervision_type, charge_category, district, officer,
        person_id, person_external_id,
        gender, risk_level, age_bucket,
        race, ethnicity,
        (year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific')) 
            AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))) AS current_month
    FROM dimension_explosion
    WHERE supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
    """.format(
        description=SUPERVISION_MATRIX_BY_PERSON_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
    )

SUPERVISION_MATRIX_BY_PERSON_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_MATRIX_BY_PERSON_VIEW_NAME,
    view_query=SUPERVISION_MATRIX_BY_PERSON_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_MATRIX_BY_PERSON_VIEW.view_id)
    print(SUPERVISION_MATRIX_BY_PERSON_VIEW.view_query)
