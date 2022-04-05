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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME = \
    'supervision_absconsion_terminations_by_officer_by_month'

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION = """
 Total supervision periods terminated for absconsion by officer and by month.
 """

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH absconsions_per_officer AS (
      SELECT
        state_code,
        EXTRACT(YEAR FROM termination_date) AS year,
        EXTRACT(MONTH FROM termination_date) AS month,
        COALESCE(SPLIT(supervision_period.supervision_site, '|')[OFFSET(0)],
                 agent.district_external_id) AS district,
        agent.agent_external_id AS officer_external_id,
        COUNT(DISTINCT person_id) AS absconsion_count
      FROM `{project_id}.{state_dataset}.state_supervision_period` supervision_period
      LEFT JOIN `{project_id}.{reference_dataset}.supervision_period_to_agent_association` agent
        USING (state_code, supervision_period_id)
      WHERE termination_date IS NOT NULL
        AND supervision_period.termination_reason = 'ABSCONSION'
        AND EXTRACT(YEAR FROM termination_date) >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
        -- Do not include any investigative or informal probation supervision periods for ID
        AND (state_code != 'US_ID'
             OR supervision_period_supervision_type NOT IN ('INVESTIGATION', 'INFORMAL_PROBATION'))
      GROUP BY state_code, year, month, district, officer_external_id
    ),
    officers_with_supervision AS (
      -- Get all officers with supervision caseloads per month
      SELECT DISTINCT
        state_code, year, month,
        SPLIT(district, '|')[OFFSET(0)] AS district,
        officer_external_id
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`
      WHERE district != 'ALL'
        AND (state_code != 'US_ID' OR supervision_type NOT IN ('ALL', 'INVESTIGATION', 'INFORMAL_PROBATION'))
    )
    SELECT
      state_code, year, month,
      officer_external_id, district,
      IFNULL(absconsions_per_officer.absconsion_count, 0) AS absconsions
    FROM officers_with_supervision
    LEFT JOIN absconsions_per_officer
      USING (state_code, year, month, district, officer_external_id)
    ORDER BY state_code, year, month, district, officer_external_id
    """

SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
