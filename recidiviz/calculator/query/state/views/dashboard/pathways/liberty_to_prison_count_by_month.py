#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""Incarcerations by month."""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_supervision_dimension_combinations import (
    PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.transition_template import (
    transition_monthly_aggregate_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LIBERTY_TO_PRISON_COUNT_BY_MONTH_VIEW_NAME = "liberty_to_prison_count_by_month"

LIBERTY_TO_PRISON_COUNT_BY_MONTH_DESCRIPTION = (
    """Admissions to prison from liberty by month."""
)

aggregate_query = """
    SELECT
        transitions.state_code,
        year,
        month,
        gender,
        age_group,
        race,
        judicial_district,
        COUNT(1) as event_count
    FROM
        `{project_id}.{dashboard_views_dataset}.liberty_to_prison_transitions` transitions,
        UNNEST ([gender, 'ALL']) AS gender,
        UNNEST ([age_group, 'ALL']) AS age_group,
        UNNEST ([race, "ALL"]) AS race,
        UNNEST ([judicial_district, 'ALL']) AS judicial_district
    GROUP BY 1, 2, 3, 4, 5, 6, 7"""

dimensions = [
    "gender",
    "age_group",
    "race",
    "judicial_district",
]


LIBERTY_TO_PRISON_COUNT_BY_MONTH_QUERY_TEMPLATE = transition_monthly_aggregate_template(
    aggregate_query,
    dimensions,
    PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)

LIBERTY_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=LIBERTY_TO_PRISON_COUNT_BY_MONTH_VIEW_NAME,
    view_query_template=LIBERTY_TO_PRISON_COUNT_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", *dimensions),
    description=LIBERTY_TO_PRISON_COUNT_BY_MONTH_DESCRIPTION,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LIBERTY_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER.build_and_print()
