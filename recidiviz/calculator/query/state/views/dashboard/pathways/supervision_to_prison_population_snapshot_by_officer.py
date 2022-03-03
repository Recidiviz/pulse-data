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
"""Admissions from supervision to prison aggregated over different time periods and grouped by supervising officer."""

from recidiviz.calculator.query.bq_utils import get_binned_time_period_months
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_supervision_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_NAME = (
    "supervision_to_prison_population_snapshot_by_officer"
)

SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_DESCRIPTION = """Admissions from supervision to prison aggregated over different time periods and grouped by supervising officer."""


SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_QUERY_TEMPLATE = """
    /* {description} */
    WITH
    data_freshness AS ({get_pathways_supervision_last_updated_date})
    , event_counts AS (
        SELECT
            transitions.state_code,
            {transition_time_period} AS time_period,
            gender,
            supervision_type,
            age_group,
            race,
            district,
            IFNULL(supervision_level, "EXTERNAL_UNKNOWN") AS supervision_level,
            officer_name,
            COUNT(1) as event_count,
        FROM `{project_id}.{shared_metric_views_dataset}.supervision_to_prison_transitions` transitions,
            UNNEST ([gender, 'ALL']) AS gender,
            UNNEST ([supervision_type, 'ALL']) AS supervision_type,
            UNNEST ([supervision_level, 'ALL']) AS supervision_level,
            UNNEST ([age_group, 'ALL']) AS age_group,
            UNNEST ([prioritized_race_or_ethnicity, "ALL"]) AS race
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_supervision_location_name_map` location
            ON transitions.state_code = location.state_code 
            AND transitions.level_1_location_external_id = location.location_id,
            UNNEST ([IFNULL(location_name, level_1_location_external_id), "ALL"]) AS district
        LEFT JOIN `{project_id}.{reference_dataset}.agent_external_id_to_full_name` agent ON
            transitions.state_code = agent.state_code
            AND transitions.supervising_officer = agent.external_id,
            UNNEST([INITCAP(given_names || ' ' || surname), "ALL"]) AS officer_name
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )

    SELECT
        last_updated,
        event_counts.*,
    FROM event_counts
    LEFT JOIN data_freshness USING (state_code)
"""

SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_NAME,
    view_query_template=SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "time_period",
        "supervision_type",
        "gender",
        "age_group",
        "race",
        "district",
        "supervision_level",
        "officer_name",
    ),
    description=SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_DESCRIPTION,
    get_pathways_supervision_last_updated_date=get_pathways_supervision_last_updated_date(),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    transition_time_period=get_binned_time_period_months("transition_date"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER.build_and_print()
