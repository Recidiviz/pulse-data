#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Prison to supervision count by month"""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import DASHBOARD_VIEWS_DATASET
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_prison_dimension_combinations import (
    PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.transition_template import (
    transition_monthly_aggregate_template,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_TO_SUPERVISION_COUNT_BY_MONTH_NAME = "prison_to_supervision_count_by_month"

PRISON_TO_SUPERVISION_COUNT_BY_MONTH_DESCRIPTION = (
    """Prison to supervision count by month"""
)

aggregate_query = """
    SELECT
        transitions.state_code,
        EXTRACT(YEAR FROM transition_date) as year,
        EXTRACT(MONTH FROM transition_date) as month,
        gender,
        age_group,
        facility,
        COUNT(1) as event_count
    FROM
        `{project_id}.{reference_dataset}.prison_to_supervision_transitions` transitions,
        UNNEST ([gender, 'ALL']) AS gender,
        UNNEST ([age_group, 'ALL']) AS age_group
    LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` location
        ON transitions.state_code = location.state_code 
        AND transitions.level_1_location_external_id = location.location_id,
        UNNEST ([IFNULL(location_id, level_1_location_external_id), "ALL"]) AS facility
    GROUP BY 1, 2, 3, 4, 5, 6
"""

dimensions = [
    "gender",
    "age_group",
    "facility",
]

PRISON_TO_SUPERVISION_COUNT_BY_MONTH_QUERY_TEMPLATE = (
    transition_monthly_aggregate_template(
        aggregate_query,
        dimensions,
        PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_NAME,
    )
)

PRISON_TO_SUPERVISION_COUNT_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_TO_SUPERVISION_COUNT_BY_MONTH_NAME,
    view_query_template=PRISON_TO_SUPERVISION_COUNT_BY_MONTH_QUERY_TEMPLATE,
    description=PRISON_TO_SUPERVISION_COUNT_BY_MONTH_DESCRIPTION,
    dimensions=("state_code", "year", "month", *dimensions),
    dashboard_views_dataset=DASHBOARD_VIEWS_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_TO_SUPERVISION_COUNT_BY_MONTH_VIEW_BUILDER.build_and_print()
