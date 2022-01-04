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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_NAME = "us_id_ppo_metrics_supervision_level"

US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_DESCRIPTION = """Metric capturing number of people on limited or low supervision on the last day of each month for the last 2 years"""

US_ID_PPO_METRICS_SUPERVISION_LEVEL_QUERY_TEMPLATE = """
    /*{description}*/

    SELECT
      state_code,
      date_of_supervision,
      supervision_type,
      COUNT(DISTINCT person_id) as count,
      COUNT(DISTINCT IF(supervision_level in ('LIMITED', 'MINIMUM'), person_id, NULL))/COUNT(*) as pct_low_or_limited_supervision
      FROM
      (
        SELECT state_code, person_id, supervision_type, supervision_level, date_of_supervision
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`
        WHERE (
          date_of_supervision = CURRENT_DATE('US/Eastern') OR
          date_of_supervision = LAST_DAY(date_of_supervision, MONTH)
        )
      )

      WHERE state_code = 'US_ID'
        AND date_of_supervision >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 3 YEAR)

      GROUP BY 1,2,3
      ORDER BY 1,2,3
    """

US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_NAME,
    view_query_template=US_ID_PPO_METRICS_SUPERVISION_LEVEL_QUERY_TEMPLATE,
    description=US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER.build_and_print()
