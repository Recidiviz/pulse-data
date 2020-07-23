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
"""Revocations Matrix by Person."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_BY_PERSON_VIEW_NAME = 'revocations_matrix_by_person'

REVOCATIONS_MATRIX_BY_PERSON_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation per person.
 This lists all individuals admitted to prison for a revocation of supervision, with number of
 violations leading up to the revocation and the most severe violation.
 """

REVOCATIONS_MATRIX_BY_PERSON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH revocations AS (
        SELECT
            state_code, year, month, metric_period_months,
            {most_severe_violation_type_subtype_grouping},
            IF(response_count > 8, 8, response_count) as reported_violations,
            most_severe_response_decision AS officer_recommendation,
            violation_history_description AS violation_record,
            person_id, person_external_id,
            gender,
            IFNULL(assessment_score_bucket, 'OVERALL') AS risk_level,
            age_bucket,
            race, ethnicity,
            supervision_type,
            charge_category,
            district,
            supervising_officer_external_id AS officer
        FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
        JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
            USING (state_code, job_id, year, month, metric_period_months),
        {district_dimension},
        {supervision_dimension},
        {charge_category_dimension}
        WHERE methodology = 'PERSON'
            AND month IS NOT NULL
            AND revocation_type = 'REINCARCERATION'
            AND person_id IS NOT NULL
            AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
            AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
    )
    SELECT
        state_code, year, month, metric_period_months,
        violation_type, reported_violations,
        officer_recommendation, violation_record,
        supervision_type, charge_category, district, officer,
        person_id, person_external_id,
        gender, age_bucket,
        -- TODO(3135): remove this aggregation once the dashboard supports LOW_MEDIUM
        CASE WHEN risk_level = 'LOW_MEDIUM' THEN 'LOW' ELSE risk_level END AS risk_level,
        race, ethnicity,
        (year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
            AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))) AS current_month
    FROM revocations
    WHERE supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
        AND district IS NOT NULL
    """

REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_TABLES_DATASET,
    view_id=REVOCATIONS_MATRIX_BY_PERSON_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_BY_PERSON_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_BY_PERSON_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    most_severe_violation_type_subtype_grouping=bq_utils.most_severe_violation_type_subtype_grouping(),
    district_dimension=bq_utils.unnest_district(),
    supervision_dimension=bq_utils.unnest_supervision_type(),
    charge_category_dimension=bq_utils.unnest_charge_category(),
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER.build_and_print()
