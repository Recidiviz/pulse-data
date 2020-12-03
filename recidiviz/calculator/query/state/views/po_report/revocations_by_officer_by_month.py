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
"""Total new crime and technical revocation revocations per officer by month."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME = 'revocations_by_officer_by_month'

REVOCATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION = """
 Revocation recommendations by officer by month.
 Counts all individuals with a revocation recommendation on probation or parole for technical or crime violations
 """

REVOCATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH revocation_recommendations AS (
      SELECT
        violation.state_code,
        EXTRACT(YEAR FROM violation.response_date) AS year,
        EXTRACT(MONTH FROM violation.response_date) AS month,
        violation.person_id,
        type.violation_type,
        agent.agent_external_id AS officer_external_id
      FROM `{project_id}.{state_dataset}.state_supervision_violation_response` violation
      LEFT JOIN `{project_id}.{state_dataset}.state_supervision_violation_response_decision_entry` decision
        USING (supervision_violation_response_id, person_id, state_code)
      LEFT JOIN `{project_id}.{state_dataset}.state_supervision_violation_type_entry` type
        USING (supervision_violation_id, person_id, state_code)
      LEFT JOIN `{project_id}.{state_dataset}.state_supervision_period` period
        -- Find the overlapping supervision periods for this violation report
        ON period.person_id = violation.person_id
            AND period.state_code = violation.state_code
            AND violation.response_date >= period.start_date
            AND violation.response_date <= COALESCE(period.termination_date, '9999-12-31')
      LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_period_to_agent_association` agent
        ON period.supervision_period_id = agent.supervision_period_id
          AND period.state_code = agent.state_code
      WHERE type.violation_type IN ('TECHNICAL', 'FELONY', 'MISDEMEANOR', 'LAW')
        AND decision.decision IN ('REVOCATION')
        AND period.supervision_period_supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
    ),
    most_severe_violation_ranking AS (
        SELECT
            state_code, year, month, officer_external_id, person_id, violation_type,
            RANK() OVER(PARTITION BY person_id, year, month, officer_external_id ORDER BY (
            CASE
                WHEN violation_type IN ('FELONY', 'MISDEMEANOR', 'LAW') THEN 1
                WHEN violation_type IN ('TECHNICAL') THEN 2
                ELSE 3
            END)
        ) AS most_severe_violation_type_rank
        FROM revocation_recommendations
    ),
    revocation_recommendations_per_officer AS (
      SELECT
        state_code, year, month,
        officer_external_id,
        COUNT(DISTINCT IF(violation_type IN ('FELONY', 'MISDEMEANOR', 'LAW'), person_id, NULL)) AS crime_revocations,
        COUNT(DISTINCT IF(violation_type = 'TECHNICAL', person_id, NULL)) AS technical_revocations
      FROM most_severe_violation_ranking
      -- Count each person_id once for the most severe violation type
      WHERE most_severe_violation_type_rank = 1
      GROUP BY state_code, year, month, officer_external_id
    ),
    avg_revocation_recommendations_by_district_state AS (
      -- Get the average monthly crime and technical revocations by district and state
      SELECT 
        state_code, year, month,
        district,
        AVG(IFNULL(crime_revocations, 0)) AS avg_crime_revocations,
        AVG(IFNULL(technical_revocations, 0)) AS avg_technical_revocations
      FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
      LEFT JOIN revocation_recommendations_per_officer
        USING (state_code, year, month, officer_external_id),
      {district_dimension}
      GROUP BY state_code, year, month, district
    )
    SELECT
      state_code, year, month,
      officer_external_id, district,
      IFNULL(revocation_recommendations_per_officer.crime_revocations, 0) AS crime_revocations,
      district_avg.avg_crime_revocations AS crime_revocations_district_average, 
      state_avg.avg_crime_revocations AS crime_revocations_state_average, 
      IFNULL(revocation_recommendations_per_officer.technical_revocations, 0) AS technical_revocations,
      district_avg.avg_technical_revocations AS technical_revocations_district_average, 
      state_avg.avg_technical_revocations AS technical_revocations_state_average, 
    FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
    LEFT JOIN revocation_recommendations_per_officer
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN (
      SELECT * FROM avg_revocation_recommendations_by_district_state
      WHERE district != 'ALL'
    ) district_avg
      USING (state_code, year, month, district)
    LEFT JOIN (
      SELECT * EXCEPT (district) FROM avg_revocation_recommendations_by_district_state
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
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='district'),
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
