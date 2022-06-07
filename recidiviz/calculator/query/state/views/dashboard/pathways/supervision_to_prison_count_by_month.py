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
"""Reincarcerations by month."""

from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
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

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_NAME = "supervision_to_prison_count_by_month"

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_DESCRIPTION = (
    """Admissions to prison from supervision by month."""
)

# TODO(#10742): implement violation fields
aggregate_query = """
    WITH base_data AS (
        SELECT
            transitions.state_code,
            EXTRACT(YEAR FROM transitions.transition_date) as year,
            EXTRACT(MONTH FROM transitions.transition_date) as month,
            transitions.gender,
            transitions.supervision_type,
            transitions.age_group,
            transitions.race,
            transitions.district,
            # TODO(#11020) Re-enable supervision_level once BE has been updated to handle larger metric files
            "ALL" AS supervision_level,
            # transitions.supervision_level,
            "ALL" AS most_severe_violation,
            "ALL" AS number_of_violations,

        FROM `{project_id}.{dashboard_views_dataset}.supervision_to_prison_transitions` transitions
    ),
    filtered_rows AS (
        SELECT * FROM base_data
        WHERE {state_specific_district_filter}
    )
    SELECT 
        state_code,
        year,
        month,
        gender,
        supervision_type,
        age_group,
        race,
        district,
        supervision_level,
        most_severe_violation,
        number_of_violations,
        COUNT(1) as event_count,
    FROM filtered_rows,
    UNNEST ([gender, 'ALL']) AS gender,
    UNNEST ([supervision_type, 'ALL']) AS supervision_type,
    UNNEST ([age_group, 'ALL']) AS age_group,
    UNNEST ([race, "ALL"]) AS race,
    UNNEST ([district, "ALL"]) AS district
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
"""

dimensions = [
    "supervision_type",
    "gender",
    "age_group",
    "race",
    "district",
    "supervision_level",
    "most_severe_violation",
    "number_of_violations",
]


SUPERVISION_TO_PRISON_COUNT_BY_MONTH_QUERY_TEMPLATE = (
    transition_monthly_aggregate_template(
        aggregate_query,
        dimensions,
        PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
    )
)

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_TO_PRISON_COUNT_BY_MONTH_QUERY_TEMPLATE,
    # year must come before month to export correctly
    dimensions=("state_code", "year", "month", *dimensions),
    metric_stats=("event_count",),
    description=SUPERVISION_TO_PRISON_COUNT_BY_MONTH_DESCRIPTION,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    state_specific_district_filter=state_specific_query_strings.pathways_state_specific_supervision_district_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER.build_and_print()
