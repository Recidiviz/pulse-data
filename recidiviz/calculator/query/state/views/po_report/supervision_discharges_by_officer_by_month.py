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
"""Supervision termination for successful discharge per officer, district, and state, by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_VIEW_NAME = 'supervision_discharges_by_officer_by_month'

SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_DESCRIPTION = """
 Discharged supervision period metrics aggregated by termination month. Includes total positive discharges per officer,
 average discharges per district, and average discharges per state.

 Officer monthly total: WHERE district != 'ALL' and officer_external_id != 'ALL'
 District average: WHERE district = DISTRICT_VALUE and officer_external_id = 'ALL'
 State average: WHERE district = 'ALL' and officer_external_id = 'ALL'
 """

SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH discharges AS (
      SELECT
        state_code, person_id, start_date, termination_date,
        supervision_period_id, supervision_site
      FROM `{project_id}.state.state_supervision_period` p1
      WHERE termination_date IS NOT NULL
         AND termination_reason = 'DISCHARGE'
    ),
    overlapping_open_period AS (
      -- Find the supervision periods that should not be counted as discharges because of another overlapping period
      SELECT
        p1.supervision_period_id
      FROM discharges p1
      JOIN `{project_id}.state.state_supervision_period` p2
        USING (state_code, person_id)
      WHERE p1.supervision_period_id != p2.supervision_period_id
        -- Find any overlapping supervision period (started on or before the discharge, ended on or after the discharge)
        AND p2.start_date <= p1.termination_date
        AND p1.termination_date <= COALESCE(p2.termination_date, CURRENT_DATE())
        -- Count the period as an overlapping open period if it wasn't discharged in the same month
        AND (p2.termination_reason != 'DISCHARGE'
            OR DATE_TRUNC(p2.termination_date, MONTH) != DATE_TRUNC(p1.termination_date, MONTH))
    )
    SELECT
      state_code, year, month,
      district, officer_external_id,
      -- This AVG is a no-op when district != 'ALL' and officer_external_id != 'ALL'
      AVG(discharge_count) AS discharge_count
    FROM (
      SELECT
        state_code,
        EXTRACT(YEAR FROM termination_date) AS year,
        EXTRACT(MONTH FROM termination_date) AS month,
        COALESCE(discharges.supervision_site, agent.district_external_id) AS district,
        agent.agent_external_id AS officer_external_id,
        COUNT(DISTINCT person_id) AS discharge_count
      FROM discharges
      LEFT JOIN overlapping_open_period USING (supervision_period_id)
      LEFT JOIN `{project_id}.{reference_dataset}.supervision_period_to_agent_association` agent
        USING (state_code, supervision_period_id)
      WHERE EXTRACT(YEAR FROM termination_date) >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
        -- Do not count any discharges that are overlapping with another open supervision period
        AND overlapping_open_period.supervision_period_id IS NULL
      GROUP BY state_code, year, month, district, officer_external_id
    ),
    {district_dimension},
    {officer_dimension}
    GROUP BY state_code, year, month, district, officer_external_id
    ORDER BY state_code, year, month, district, officer_external_id
    """

SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='district'),
    officer_dimension=bq_utils.unnest_column(input_column_name='officer_external_id',
                                             output_column_name='officer_external_id'),
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
