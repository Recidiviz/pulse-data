# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Total requests for early discharge terminations per officer, district, and state, by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_VIEW_NAME = \
    'supervision_early_discharge_requests_by_officer_by_month'

SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_DESCRIPTION = """
 Early discharge requests for supervision periods by request month. Includes total requests per officer,
 average requests per district, and average requests per state.
 """

SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH requests_per_officer AS (
      -- Gather all early discharges, joined with the supervising officer and district
      SELECT
        state_code,
        EXTRACT(YEAR FROM request_date) AS year,
        EXTRACT(MONTH FROM request_date) AS month,
        -- TODO(#4491): Consider using `external_id` instead of `agent_external_id`
        COALESCE(agent.agent_external_id, 'UNKNOWN') AS officer_external_id,
        COUNT(DISTINCT period.person_id) AS earned_discharges
      FROM `{project_id}.{state_dataset}.state_early_discharge` discharge
      JOIN `{project_id}.{state_dataset}.state_supervision_period` period
        USING (state_code, person_id)
      LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_period_to_agent_association` agent
        USING (state_code, supervision_period_id)
      -- Attribute an early discharge to a supervision period when it was requested before the period was terminated
      WHERE period.start_date <= discharge.request_date
        AND discharge.request_date < COALESCE(period.termination_date, '9999-12-31')
        -- Only the following supervision types should be included in the PO report
        AND supervision_period_supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
      GROUP BY state_code, year, month, officer_external_id
    ),
    avg_requests_by_district_state AS (
      -- Get the average monthly discharges by district and state
      SELECT
        state_code, year, month,
        district,
        AVG(IFNULL(earned_discharges, 0)) AS avg_earned_discharges
      FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
      LEFT JOIN requests_per_officer
        USING (state_code, year, month, officer_external_id),
      {district_dimension}
      GROUP BY state_code, year, month, district
    )
    SELECT
      state_code, year, month,
      officer_external_id, district,
      IFNULL(requests_per_officer.earned_discharges, 0) AS earned_discharges,
      district_avg.avg_earned_discharges AS earned_discharges_district_average,
      state_avg.avg_earned_discharges AS earned_discharges_state_average
    FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
    LEFT JOIN requests_per_officer
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN (
      SELECT * FROM avg_requests_by_district_state
      WHERE district != 'ALL'
    ) district_avg
      USING (state_code, year, month, district)
    LEFT JOIN (
      SELECT * EXCEPT (district) FROM avg_requests_by_district_state
      WHERE district = 'ALL'
    ) state_avg
      USING (state_code, year, month)
    ORDER BY state_code, year, month, district, officer_external_id
    """

SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='district'),
    po_report_dataset=dataset_config.PO_REPORT_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
