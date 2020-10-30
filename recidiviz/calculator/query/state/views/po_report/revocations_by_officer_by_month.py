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
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
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
        supervising_officer_external_id AS officer_external_id,
        most_severe_violation_type
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
      {filter_to_most_recent_job_id_for_metric}
      WHERE methodology = 'PERSON'
       AND metric_period_months = 1
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    ),
    revocations_per_officer AS (
      SELECT
        state_code, year, month,
        officer_external_id,
        COUNT(DISTINCT IF(most_severe_violation_type IN ('FELONY', 'MISDEMEANOR', 'LAW'), person_id, NULL)) AS crime_revocations,
        COUNT(DISTINCT IF(most_severe_violation_type = 'TECHNICAL', person_id, NULL)) AS technical_revocations
      FROM revocations
      GROUP BY state_code, year, month, officer_external_id
    ),
    avg_revocations_by_district_state AS (
      -- Get the average monthly crime and technical revocations by district and state
      SELECT 
        state_code, year, month,
        district,
        AVG(IFNULL(crime_revocations, 0)) AS avg_crime_revocations,
        AVG(IFNULL(technical_revocations, 0)) AS avg_technical_revocations
      FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
      LEFT JOIN revocations_per_officer
        USING (state_code, year, month, officer_external_id),
      {district_dimension}
      GROUP BY state_code, year, month, district
    )
    SELECT
      state_code, year, month,
      officer_external_id, district,
      IFNULL(revocations_per_officer.crime_revocations, 0) AS crime_revocations,
      district_avg.avg_crime_revocations AS crime_revocations_district_average, 
      state_avg.avg_crime_revocations AS crime_revocations_state_average, 
      IFNULL(revocations_per_officer.technical_revocations, 0) AS technical_revocations,
      district_avg.avg_technical_revocations AS technical_revocations_district_average, 
      state_avg.avg_technical_revocations AS technical_revocations_state_average, 
    FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
    LEFT JOIN revocations_per_officer
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN (
      SELECT * FROM avg_revocations_by_district_state
      WHERE district != 'ALL'
    ) district_avg
      USING (state_code, year, month, district)
    LEFT JOIN (
      SELECT * EXCEPT (district) FROM avg_revocations_by_district_state
      WHERE district = 'ALL'
    ) state_avg
      USING (state_code, year, month)
    ORDER BY state_code, year, month, district, officer_external_id
    """

REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=REVOCATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=REVOCATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='district'),
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
