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
"""Prison to supervision person level population snapshot by dimension"""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_NAME = (
    "prison_to_supervision_population_snapshot_person_level"
)

PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_DESCRIPTION = """
    Prison to supervision person level population snapshot by dimension.
    Individuals are not deduplicated; they will appear once per associated event."""

PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    data_freshness AS ({last_updated_query})

    SELECT
        state_code,
        last_updated,
        facility,
        gender,
        age,
        age_group,
        time_period,
        state_id,
        full_name,
        race,
    FROM `{project_id}.{dashboard_views_dataset}.prison_to_supervision_transitions` transitions
    LEFT JOIN data_freshness USING (state_code)
    WHERE time_period IS NOT NULL
        
"""

PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_NAME,
    view_query_template=PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_QUERY_TEMPLATE,
    description=PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_DESCRIPTION,
    dimensions=(
        "state_code",
        "gender",
        "facility",
        "age_group",
        "time_period",
        "state_id",
        "race",
    ),
    metric_stats=("last_updated",),
    metric_metadata=(
        "age",
        "full_name",
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    last_updated_query=get_pathways_incarceration_last_updated_date(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
