# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Admissions to prison from supervision aggregated over different time periods."""

from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_supervision_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "supervision_to_prison_population_snapshot_by_dimension"
)

SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Admissions to prison from supervision aggregated over different time periods."""
)


# TODO(#10742): implement violation fields
SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /* {description} */
    WITH
    data_freshness AS ({get_pathways_supervision_last_updated_date})
    ,
    transitions AS (
        SELECT
            transitions.state_code,
            transitions.time_period,
            transitions.gender,
            transitions.supervision_type,
            transitions.age_group,
            transitions.race,
            transitions.supervision_district AS district,
            transitions.length_of_stay,
            transitions.supervision_level,
            "ALL" AS most_severe_violation,
            "ALL" AS number_of_violations,
        FROM `{project_id}.{dashboard_views_dataset}.supervision_to_prison_transitions` transitions
    ),
    filtered_rows AS (
        SELECT * FROM transitions
        WHERE {state_specific_district_filter}
    ),
    event_counts AS (
        SELECT
            {dimensions_clause},
            COUNT(1) as event_count,
        FROM filtered_rows,
            UNNEST ([gender, 'ALL']) AS gender,
            UNNEST ([supervision_type, 'ALL']) AS supervision_type,
            UNNEST ([supervision_level, 'ALL']) AS supervision_level,
            UNNEST ([age_group, 'ALL']) AS age_group,
            UNNEST ([race, "ALL"]) AS race,
            UNNEST ([district, "ALL"]) AS district,
            UNNEST ([length_of_stay, "ALL"]) AS length_of_stay
        WHERE time_period IS NOT NULL
        AND length_of_stay != "UNKNOWN"
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    )

    SELECT
        last_updated,
        event_counts.*,
    FROM event_counts
    LEFT JOIN data_freshness USING (state_code)
"""

SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "time_period",
        "supervision_type",
        "gender",
        "age_group",
        "race",
        "district",
        "length_of_stay",
        "supervision_level",
        "most_severe_violation",
        "number_of_violations",
    ),
    description=SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    get_pathways_supervision_last_updated_date=get_pathways_supervision_last_updated_date(),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    state_specific_district_filter=state_specific_query_strings.pathways_state_specific_supervision_district_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
