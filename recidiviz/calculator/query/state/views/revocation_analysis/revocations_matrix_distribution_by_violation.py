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

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_NAME = 'revocations_matrix_distribution_by_violation'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_DESCRIPTION = """
 Relative frequency of each type of violation and condition violated for people who were revoked to prison. This is
 calculated as the total number of times each type of violation and condition violated was reported on all violations
 filed during a period of 12 months leading up to revocation, divided by the total number of notices of citation and
 violation reports filed during that period. 
 """

# TODO(2853): Handle unset violation type in the calc step
REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_QUERY = \
    """
    /*{description}*/
    SELECT
        state_code,
        year,
        month,
        metric_period_months,
        supervision_type,
        charge_category,
        district,
        IF(response_count > 8, 8, response_count) as reported_violations,
        CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN
          CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
               WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
               ELSE most_severe_violation_type END
          ELSE most_severe_violation_type
          END AS violation_type,
        SUM(IF(violation_count_type = 'ABSCONDED', count, 0)) AS absconded_count,
        SUM(IF(violation_count_type = 'ASC', count, 0)) AS association_count,
        SUM(IF(violation_count_type = 'DIR', count, 0)) AS directive_count,
        SUM(IF(violation_count_type = 'DRG', count, 0)) AS substance_count,
        SUM(IF(violation_count_type = 'EMP', count, 0)) AS employment_count,
        SUM(IF(violation_count_type = 'FELONY', count, 0)) AS felony_count,
        SUM(IF(violation_count_type = 'INT', count, 0)) AS intervention_fee_count,
        SUM(IF(violation_count_type IN ('LAW_CITATION', 'MISDEMEANOR'), count, 0)) AS misdemeanor_count,
        SUM(IF(violation_count_type = 'MUNICIPAL', count, 0)) AS municipal_count,
        SUM(IF(violation_count_type = 'RES', count, 0)) AS residency_count,
        SUM(IF(violation_count_type = 'SPC', count, 0)) AS special_count,
        SUM(IF(violation_count_type = 'SUP', count, 0)) AS supervision_strategy_count,
        SUM(IF(violation_count_type = 'TRA', count, 0)) AS travel_count,
        SUM(IF(violation_count_type = 'WEA', count, 0)) AS weapon_count,
        SUM(IF(violation_count_type = 'VIOLATION', count, 0)) AS violation_count
    FROM `{project_id}.{metrics_dataset}.supervision_revocation_violation_type_analysis_metrics`
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months),
    {district_dimension},
    {supervision_dimension},
    {charge_category_dimension}
    WHERE revocation_type = 'REINCARCERATION'
        AND methodology = 'PERSON'
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_REVOCATION_VIOLATION'
        AND district IS NOT NULL
    GROUP BY state_code, year, month, metric_period_months, supervision_type, charge_category, district, response_count,
        violation_type
    ORDER BY state_code, year, month, metric_period_months, supervision_type, district, charge_category, violation_type,
        response_count
    """.format(
        description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
        district_dimension=bq_utils.unnest_district(),
        supervision_dimension=bq_utils.unnest_supervision_type(),
        charge_category_dimension=bq_utils.unnest_charge_category(),
    )

REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW.view_id)
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW.view_query)
