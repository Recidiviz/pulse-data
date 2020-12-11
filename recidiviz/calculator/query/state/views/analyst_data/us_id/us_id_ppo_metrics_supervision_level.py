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
"""Metric capturing number of people on limited or low supervision on the last day of each month"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET, ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_NAME = 'us_id_ppo_metrics_supervision_level'

US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_DESCRIPTION = \
    """Metric capturing number of people on limited or low supervision on the last day of each month for the last 2 years"""

US_ID_PPO_METRICS_SUPERVISION_LEVEL_QUERY_TEMPLATE = \
    """
    /*{description}*/

    SELECT 
      state_code,
      date_of_supervision,
      supervision_type,
      COUNT(*) as count,
      COUNTIF(supervision_level in ('LIMITED', 'MINIMUM'))/COUNT(*) as pct_low_or_limited_supervision
      FROM 
      (
        SELECT state_code, person_id, supervision_type, supervision_level, date_of_supervision
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics` 
        JOIN `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
            USING (job_id, state_code, year, month, metric_period_months, metric_type)
        WHERE methodology = 'PERSON'
          AND metric_period_months = 0
          AND month IS NOT NULL
          AND (date_of_supervision = CURRENT_DATE() OR is_on_supervision_last_day_of_month)      
      )
      
      WHERE state_code = 'US_ID'
        AND date_of_supervision >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
        
      GROUP BY 1,2,3
      ORDER BY 1,2,3
    """

US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_NAME,
    view_query_template=US_ID_PPO_METRICS_SUPERVISION_LEVEL_QUERY_TEMPLATE,
    description=US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER.build_and_print()
