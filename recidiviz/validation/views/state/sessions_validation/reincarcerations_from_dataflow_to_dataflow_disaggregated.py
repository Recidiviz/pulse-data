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

"""A view which provides a person / day level comparison between incarceration session starts and dataflow admissions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME = (
    "reincarcerations_from_dataflow_to_dataflow_disaggregated"
)

REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION = """
A view which provides a person / day level comparison between session identified reincarcerations and the 
recidivism count dataflow metric. For each person / reincarceration date there are a set of binary variables that 
indicate whether the reincarceration appears in dataflow, sessions, or both. Additionally, there is a field that
indicates whether the number of days between the release and the reincarceration match.
"""

REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        person_id,
        state_code,
        reincarceration_date,
        s.release_to_reincarceration_days AS sessions_days_at_liberty,
        d.days_at_liberty AS dataflow_days_at_liberty,
        CASE WHEN s.reincarceration_date IS NOT NULL THEN 1 ELSE 0 END AS in_sessions,
        CASE WHEN d.reincarceration_date IS NOT NULL THEN 1 ELSE 0 END AS in_dataflow,
        CASE WHEN s.reincarceration_date IS NOT NULL AND d.reincarceration_date IS NOT NULL THEN 1 ELSE 0 END AS in_both,
        CASE WHEN s.release_to_reincarceration_days = d.days_at_liberty THEN 1 ELSE 0 END AS release_days_match
    FROM
      (
      SELECT *
      FROM `{project_id}.{sessions_dataset}.reincarceration_sessions_from_dataflow_materialized` 
      WHERE reincarceration_date IS NOT NULL
      ) AS s
    FULL OUTER JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_recidivism_count_metrics_materialized` d
        USING(person_id, state_code, reincarceration_date)
    WHERE EXTRACT(YEAR FROM reincarceration_date) > EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR))
    ORDER BY 1,2,3
    """

REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME,
    view_query_template=REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE,
    description=REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.build_and_print()
