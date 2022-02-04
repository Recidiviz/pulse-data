# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Releases from supervision by month."""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_supervision_dimension_combinations import (
    PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.transition_template import (
    transition_monthly_aggregate_template,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_VIEW_NAME = (
    "supervision_to_liberty_count_by_month"
)

SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_DESCRIPTION = (
    """Releases from supervision to liberty by month."""
)

aggregate_query = """
SELECT
    transitions.state_code,
    EXTRACT(YEAR FROM transition_date) as year,
    EXTRACT(MONTH FROM transition_date) as month,
    gender,
    supervision_type,
    IFNULL(supervision_level, "EXTERNAL_UNKNOWN") AS supervision_level,
    age_group,
    race,
    district,
    COUNT(1) as event_count,
FROM
    `{project_id}.{reference_dataset}.supervision_to_liberty_transitions` transitions,
    UNNEST ([gender, 'ALL']) AS gender,
    UNNEST ([supervision_type, 'ALL']) AS supervision_type,
    UNNEST ([supervision_level, "ALL"]) AS supervision_level,
    UNNEST ([age_group, 'ALL']) AS age_group,
    UNNEST ([prioritized_race_or_ethnicity, "ALL"]) AS race
LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_supervision_location_name_map` location
    ON transitions.state_code = location.state_code 
    AND transitions.district_id = location.location_id,
    UNNEST ([IFNULL(location_name, district_id), "ALL"]) AS district
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9"""

dimensions = [
    "supervision_type",
    "gender",
    "age_group",
    "race",
    "district",
    "supervision_level",
]


SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_QUERY_TEMPLATE = (
    transition_monthly_aggregate_template(
        aggregate_query,
        dimensions,
        PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
    )
)

SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", *dimensions),
    description=SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_DESCRIPTION,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_VIEW_BUILDER.build_and_print()
