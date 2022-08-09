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
"""Prison person level population """
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    filter_to_enabled_states,
    get_person_full_name,
)
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_PERSON_LEVEL_VIEW_NAME = "prison_population_person_level"

PRISON_POPULATION_PERSON_LEVEL_DESCRIPTION = """Prison person level population"""

PRISON_POPULATION_PERSON_LEVEL_QUERY_TEMPLATE = """
    /*{description}*/
    WITH all_rows AS (
        SELECT DISTINCT
            pop.state_code,
            pop.gender,
            admission_reason,
            IFNULL(aggregating_location_id, pop.facility) AS facility,
            {add_age_groups}
            pop.age,
            person_external_id AS state_id,
            {formatted_name} AS full_name,
            prioritized_race_or_ethnicity as race,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_incarceration_population_metrics_included_in_state_population_materialized` pop
        LEFT JOIN `{project_id}.{state_dataset}.state_person` person USING (person_id)
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` name_map
                ON pop.state_code = name_map.state_code
                AND pop.facility = name_map.location_id
        {filter_to_enabled_states}
    ), filtered_rows AS (
        SELECT
            state_code,
            {dimensions_clause},
            age,
            IFNULL(full_name, "Unknown") AS full_name,
        FROM all_rows
        WHERE {facility_filter}
        AND state_id IS NOT NULL
    )
    SELECT {columns}
    FROM filtered_rows
"""

PRISON_POPULATION_PERSON_LEVEL_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=state_specific_query_strings.get_pathways_incarceration_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        columns=[
            "state_code",
            "gender",
            "admission_reason",
            "facility",
            "age_group",
            "race",
            "state_id",
            "full_name",
            "age",
        ],
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=PRISON_POPULATION_PERSON_LEVEL_VIEW_NAME,
        view_query_template=PRISON_POPULATION_PERSON_LEVEL_QUERY_TEMPLATE,
        description=PRISON_POPULATION_PERSON_LEVEL_DESCRIPTION,
        dimensions_clause=PathwaysMetricBigQueryViewBuilder.replace_unknowns(
            [
                "gender",
                "admission_reason",
                "facility",
                "age_group",
                "race",
                "state_id",
            ]
        ),
        dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
        materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
        state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
        add_age_groups=add_age_groups(),
        filter_to_enabled_states=filter_to_enabled_states(
            state_code_column="pop.state_code",
            enabled_states=get_pathways_enabled_states(),
        ),
        formatted_name=get_person_full_name("person.full_name"),
        facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
