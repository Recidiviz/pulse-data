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
"""Metric capturing number of unique people receiving an early discharge request each month"""
# pylint: disable=trailing-whitespace, disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET, ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_NAME = 'us_id_ppo_metrics_early_discharge_requests'

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_DESCRIPTION = \
    """Metric capturing number of unique people receiving a valid early discharge request each month for the past 2 years"""

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      /* Convert month label from first of month to last of month. If month is not yet complete, use current date 
      instead of the last of the month to indicate last date of available data */
      LEAST(CURRENT_DATE(), LAST_DAY(request_month, MONTH)) as request_month,
      supervision_type,
      COUNT(DISTINCT ed_requested_person) as ed_request_count,

    FROM (
      SELECT
        state_code,
        --TODO(#4880): Use period supervision type rather than deciding body type raw text to determine supervision type
        deciding_body_type_raw_text as supervision_type,
        DATE_TRUNC(request_date, MONTH) AS request_month,
        IF(request_date IS NOT NULL, person_id, NULL) as ed_requested_person,
      FROM `{project_id}.{base_dataset}.state_early_discharge`
      WHERE decision_status != 'INVALID'
    )

    WHERE request_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_NAME,
    view_query_template=US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_QUERY_TEMPLATE,
    description=US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER.build_and_print()
