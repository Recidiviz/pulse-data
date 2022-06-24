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
"""Prison person level population snapshot by dimension."""
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    filter_to_enabled_states,
    get_person_full_name,
)
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_NAME = (
    "prison_population_snapshot_person_level"
)

PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_DESCRIPTION = (
    """Prison person level population snapshot by dimension"""
)

PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_QUERY_TEMPLATE = """
    /*{description}*/
    WITH get_last_updated AS ({get_pathways_incarceration_last_updated_date})
    , all_rows AS (
        SELECT DISTINCT
            pop.state_code,
            get_last_updated.last_updated,
            pop.gender,
            admission_reason AS legal_status,
            IFNULL(aggregating_location_id, pop.facility) AS facility,
            {add_age_groups}
            pop.age,
            person_external_id AS state_id,
            {formatted_name} AS full_name,
            prioritized_race_or_ethnicity as race,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_incarceration_population_metrics_included_in_state_population_materialized` pop
        LEFT JOIN get_last_updated USING (state_code)
        LEFT JOIN `{project_id}.{state_dataset}.state_person` person USING (person_id)
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` name_map
                ON pop.state_code = name_map.state_code
                AND pop.facility = name_map.location_id
        {filter_to_enabled_states}
    )
    SELECT
        {dimensions_clause},
        last_updated,
        age,
        IFNULL(full_name, "Unknown") AS full_name,
    FROM all_rows
    WHERE {facility_filter}
    AND state_id IS NOT NULL
"""

PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_NAME,
    view_query_template=PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_QUERY_TEMPLATE,
    description=PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_DESCRIPTION,
    dimensions=(
        "state_code",
        "gender",
        "legal_status",
        "facility",
        "age_group",
        "state_id",
        "race",
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    add_age_groups=add_age_groups(),
    get_pathways_incarceration_last_updated_date=get_pathways_incarceration_last_updated_date(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="pop.state_code", enabled_states=get_pathways_enabled_states()
    ),
    formatted_name=get_person_full_name("person.full_name"),
    facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_SNAPSHOT_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
