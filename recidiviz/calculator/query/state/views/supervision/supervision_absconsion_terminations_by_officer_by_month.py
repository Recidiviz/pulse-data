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
"""Supervision terminations for absconsion per officer by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME = \
    'supervision_absconsion_terminations_by_officer_by_month'

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION = """
 Total supervision periods terminated for absconsion by officer and by month.
 """

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      EXTRACT(YEAR FROM termination_date) AS year,
      EXTRACT(MONTH FROM termination_date) AS month,
      COALESCE(supervision_period.supervision_site, agent.district_external_id) as district,
      agent.agent_external_id AS officer_external_id,
      COUNT(DISTINCT person_id) AS absconsion_count
    FROM `{project_id}.state.state_supervision_period` supervision_period
    LEFT JOIN `{project_id}.{reference_dataset}.supervision_period_to_agent_association` agent
      USING (state_code, supervision_period_id)
    WHERE termination_date IS NOT NULL
      AND supervision_period.termination_reason = 'ABSCONSION'
      AND EXTRACT(YEAR FROM termination_date) >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    GROUP BY state_code, year, month, district, officer_external_id
    ORDER BY state_code, year, month, district, officer_external_id
    """

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW = BigQueryView(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    print(SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW.view_id)
    print(SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW.view_query)
