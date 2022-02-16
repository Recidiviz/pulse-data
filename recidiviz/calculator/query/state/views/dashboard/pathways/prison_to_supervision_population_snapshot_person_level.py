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
from recidiviz.calculator.query.bq_utils import (
    get_binned_time_period_months,
    get_person_full_name,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
    state_specific_external_id_type,
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
    data_freshness AS ({last_updated_query}),
    transitions AS (
        SELECT
            transitions.state_code, 
            transitions.person_id,
            external_id AS state_id,
            gender,
            age,
            age_group,
            {transition_time_period} AS time_period,
            level_1_location_external_id AS location_id,
        FROM `{project_id}.{reference_dataset}.prison_to_supervision_transitions` transitions
        LEFT JOIN `{project_id}.{state_dataset}.state_person_external_id` pei
            ON transitions.person_id = pei.person_id
            AND {state_id_type} = pei.id_type 
    )

    SELECT
        state_code,
        last_updated,
        IFNULL(aggregating_location_id, location_id) AS facility,
        transitions.gender,
        age,
        age_group,
        time_period,
        state_id,
        {formatted_name} AS full_name,
    FROM transitions
    LEFT JOIN `{project_id}.{state_dataset}.state_person` person USING (state_code, person_id)
    LEFT JOIN data_freshness USING (state_code)
    LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` 
        USING (state_code, location_id)
    WHERE 
        time_period IS NOT NULL
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
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    formatted_name=get_person_full_name("person.full_name"),
    last_updated_query=get_pathways_incarceration_last_updated_date(),
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    state_id_type=state_specific_external_id_type("transitions"),
    transition_time_period=get_binned_time_period_months("transition_date"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_TO_SUPERVISION_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
