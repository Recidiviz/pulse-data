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
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_BY_PERSON_VIEW_NAME = 'revocations_matrix_by_person'

REVOCATIONS_MATRIX_BY_PERSON_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation per person.
 This lists all individuals admitted to prison for a revocation of supervision, with number of
 violations leading up to the revocation and the most severe violation. If a person has had multiple revocation
 admissions within a metric period, the most recent revocation admission is used.
 """

REVOCATIONS_MATRIX_BY_PERSON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH revocations AS (
        SELECT
            state_code,
            metric_period_months,
            revocation_admission_date,
            {most_severe_violation_type_subtype_grouping},
            IF(response_count > 8, 8, response_count) as reported_violations,
            person_id,
            person_external_id,
            gender,
            assessment_score_bucket AS risk_level,
            age_bucket,
            race,
            ethnicity,
            supervision_type,
            case_type,
            district,
            officer,
            ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY revocation_admission_date DESC) as ranking
        FROM `{project_id}.{reference_views_dataset}.event_based_revocations_for_matrix`,
        {metric_period_dimension}
        WHERE {metric_period_condition}
    )
    SELECT
        state_code,
        metric_period_months,
        revocation_admission_date,
        violation_type,
        reported_violations,
        supervision_type,
        charge_category,
        district,
        officer,
        person_id,
        person_external_id,
        gender,
        age_bucket,
        risk_level,
        race,
        ethnicity,
    FROM revocations,
    {district_dimension},
    {supervision_dimension},
    {charge_category_dimension}
    WHERE ranking = 1
      AND supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
    """

REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_BY_PERSON_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_BY_PERSON_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_BY_PERSON_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=
    state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    district_dimension=bq_utils.unnest_district('district'),
    supervision_dimension=bq_utils.unnest_supervision_type(),
    charge_category_dimension=bq_utils.unnest_charge_category(),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition()
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER.build_and_print()
