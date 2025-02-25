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
"""Total new crime and technical revocations per officer by month."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME = 'revocations_by_officer_by_month'

REVOCATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION = """
 Revocations by officer by month.
 Counts all individuals revoked on probation or parole for technical or crime violations
 """

REVOCATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH revocations AS (
      SELECT
        state_code, year, month, person_id,
        SPLIT(supervising_district_external_id, '|')[OFFSET(0)] AS district,
        supervising_officer_external_id AS officer_external_id,
        most_severe_violation_type
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
    WHERE methodology = 'PERSON'
        AND revocation_type = 'REINCARCERATION'
        AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
        AND metric_period_months = 1
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    ),
    revocations_per_officer AS (
      SELECT
        state_code, year, month,
        officer_external_id, district,
        COUNT(DISTINCT IF(most_severe_violation_type IN ('FELONY', 'MISDEMEANOR'), person_id, NULL)) AS crime_revocations,
        COUNT(DISTINCT IF(most_severe_violation_type = 'TECHNICAL', person_id, NULL)) AS technical_revocations
      FROM revocations
      GROUP BY state_code, year, month, district, officer_external_id
    ),
    officers_with_supervision AS (
      -- Get all officers with supervision caseloads each month
      SELECT DISTINCT
        state_code, year, month,
        SPLIT(district, '|')[OFFSET(0)] AS district,
        officer_external_id
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`
      WHERE district != 'ALL'
        -- Only the following supervision types should be included in the PO report
        AND supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
    )
    SELECT
      state_code, year, month,
      officer_external_id, district,
      IFNULL(revocations_per_officer.crime_revocations, 0) AS crime_revocations,
      IFNULL(revocations_per_officer.technical_revocations, 0) AS technical_revocations
    FROM officers_with_supervision
    LEFT JOIN revocations_per_officer
      USING (state_code, year, month, district, officer_external_id)
    ORDER BY state_code, year, month, district, officer_external_id
    """

REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=REVOCATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
