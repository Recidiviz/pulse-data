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
"""Revocations Matrix Distribution by Violation."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_NAME = 'revocations_matrix_distribution_by_violation'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_DESCRIPTION = """
 Relative frequency of each type of violation and condition violated for people who were revoked to prison. This is
 calculated as the total number of times each type of violation and condition violated was reported on all violations
 filed during a period of 12 months leading up to revocation, divided by the total number of notices of citation and
 violation reports filed during that period. 
 """

# TODO(#3981): Reconfigure this view to support more than just US_MO violation categories
REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_QUERY_TEMPLATE = \
    """
    /*{description}*/
    
    /*
 Relative frequency of each type of violation and condition violated for people who were revoked to prison. This is
 calculated as the total number of times each type of violation and condition violated was reported on all violations
 filed during a period of 12 months leading up to revocation, divided by the total number of notices of citation and
 violation reports filed during that period. 
 */
    
    WITH state_specific_violation_count_types AS (
      SELECT
        state_code,
        year,
        month,
        metric_period_months,
        supervision_type,
        case_type,
        IFNULL(supervising_district_external_id, 'EXTERNAL_UNKNOWN') as supervising_district_external_id,
        IF(response_count > 8, 8, response_count) as reported_violations,
        {most_severe_violation_type_subtype_grouping},
        {violation_count_type_grouping},
        count
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_violation_type_analysis_metrics`
      JOIN `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized` job
        USING (state_code, job_id, year, month, metric_period_months, metric_type)
      WHERE revocation_type = 'REINCARCERATION'
        AND methodology = 'PERSON'
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
      )

    
    SELECT
        state_code,
        year,
        month,
        metric_period_months,
        supervision_type,
        charge_category,
        district,
        reported_violations,
        violation_type,
        -- Shared violation categories --
        SUM(IF(violation_count_type = 'ABSCONDED', count, 0)) AS absconded_count,
        SUM(IF(violation_count_type = 'FELONY', count, 0)) AS felony_count,
        SUM(IF(violation_count_type = 'MUNICIPAL', count, 0)) AS municipal_count,
        SUM(IF(violation_count_type = 'MISDEMEANOR', count, 0)) AS misdemeanor_count,
        SUM(IF(violation_count_type = 'SUBSTANCE_ABUSE', count, 0)) AS substance_count,
        -- State-specific violation categories --
        {state_specific_violation_categories},
        -- Overall violation count --
        SUM(IF(violation_count_type = 'VIOLATION', count, 0)) AS violation_count
    FROM
    state_specific_violation_count_types,
    {district_dimension},
    {supervision_type_dimension},
    {charge_category_dimension}
    GROUP BY state_code, year, month, metric_period_months, supervision_type, charge_category, district, reported_violations, violation_type
    ORDER BY state_code, year, month, metric_period_months, supervision_type, district, charge_category, violation_type, reported_violations
    """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_QUERY_TEMPLATE,
    dimensions=['state_code', 'year', 'month', 'metric_period_months', 'district', 'supervision_type',
                'violation_type', 'reported_violations', 'charge_category'],
    description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=
    state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    violation_count_type_grouping=state_specific_query_strings.state_specific_violation_count_type_grouping(),
    state_specific_violation_categories=state_specific_query_strings.state_specific_violation_count_type_categories(),
    district_dimension=bq_utils.unnest_district(),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    charge_category_dimension=bq_utils.unnest_charge_category(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_BUILDER.build_and_print()
