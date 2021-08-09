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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_NAME = (
    "us_id_ppo_metrics_early_discharge_requests"
)

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_DESCRIPTION = """Metric capturing number of unique people receiving a valid early discharge request each month for the past 3 years"""

US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      state_code,
      /* Convert month label from first of month to last of month. If month is not yet complete, use current date 
      instead of the last of the month to indicate last date of available data */
      LEAST(CURRENT_DATE(), LAST_DAY(request_month, MONTH)) as request_month,
      IF(supervision_type = 'DUAL', 'PAROLE', supervision_type) as supervision_type,
      COUNT(DISTINCT ed_requested_person) as ed_request_count,

    FROM (
      SELECT
        ed.state_code,
        /* If current session is not a supervision session, infer supervision type from inflow supervision type.
        The vast majority of the preceding supervision sessions ending in release did have a valid end reason 
        (DISCHARGE/EXPIRATION) and most of the time the request date happened to be within a few days, occasionally 
        a couple weeks after the release date, with only a very small number happening as far as a year out. 
        We think this allows for the assumption that those requests are indeed attached to the previous 
        supervision period, and that there may be a margin of error/delay in recording some early discharge requests.
        */
        IF(sessions.compartment_level_1 = 'SUPERVISION', compartment_level_2,
            IF(sessions.inflow_from_level_1 = 'SUPERVISION', sessions.inflow_from_level_2, 'UNKNOWN')
        ) as supervision_type,
        DATE_TRUNC(request_date, MONTH) AS request_month,
        IF(request_date IS NOT NULL, ed.person_id, NULL) as ed_requested_person,
      FROM `{project_id}.{base_dataset}.state_early_discharge` ed
      /* Join with overlapping session to get supervision type at time of request */
      LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        ON ed.state_code = sessions.state_code
          AND ed.person_id = sessions.person_id
          AND ed.request_date >= sessions.start_date  
          AND request_date <= COALESCE(sessions.end_date, "9999-01-01")
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
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER.build_and_print()
