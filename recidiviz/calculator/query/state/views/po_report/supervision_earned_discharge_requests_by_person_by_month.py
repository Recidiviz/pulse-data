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
"""Earned discharge requests per person per month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_VIEW_NAME = \
    'supervision_earned_discharge_requests_by_person_by_month'

SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_DESCRIPTION = """
 Earned discharge requests per person per month.
 """

# TODO(#4491): Consider using `external_id` instead of `agent_external_id`
SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT DISTINCT
      state_code, 
      EXTRACT(YEAR FROM request_date) AS year,
      EXTRACT(MONTH FROM request_date) AS month, 
      period.person_id, 
      agent.agent_external_id AS officer_external_id,
      -- Take the most recent request date
      MAX(discharge.request_date) AS earned_discharge_date
    FROM `{project_id}.{state_dataset}.state_early_discharge` discharge
    JOIN `{project_id}.{state_dataset}.state_supervision_period` period
        USING (state_code, person_id)
    LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_period_to_agent_association` agent
        USING (state_code, supervision_period_id)
    WHERE discharge.decision_status NOT IN ('INVALID')
        AND period.start_date <= discharge.request_date
        AND discharge.request_date < COALESCE(period.termination_date, '9999-12-31')
        -- Only the following supervision types should be included in the PO report
        AND supervision_period_supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
        AND agent.agent_external_id IS NOT NULL
    GROUP BY state_code, year, month, person_id, officer_external_id
    """

SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_VIEW_BUILDER.build_and_print()
