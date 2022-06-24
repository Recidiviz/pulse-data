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
"""Supervision population snapshot by dimension.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.supervision_population_snapshot_by_dimension
"""
from recidiviz.calculator.query.bq_utils import filter_to_enabled_states
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

SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "supervision_population_snapshot_by_dimension"
)

SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Supervision population snapshot by dimension"""
)

SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    get_last_updated AS ({get_pathways_supervision_last_updated_date})
    , cte AS ( 
        SELECT *
        FROM `{project_id}.{dashboards_dataset}.pathways_deduped_supervision_sessions`
        WHERE end_date IS NULL
    )
    , filtered_rows AS (
        SELECT 
            state_code,
            person_id,
            last_updated,
            district,
            supervision_level,
            race,
        FROM cte
        LEFT JOIN get_last_updated  USING (state_code)
        {filter_to_enabled_states}
        AND {state_specific_district_filter}
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    SELECT
        {dimensions_clause},
        last_updated,
        COUNT(DISTINCT person_id) as person_count,
    FROM filtered_rows,
    UNNEST([district, 'ALL']) AS district,
    UNNEST([supervision_level, 'ALL']) AS supervision_level,
    UNNEST([race, 'ALL']) AS race
    WHERE supervision_level IS NOT NULL
        AND supervision_level != "EXTERNAL_UNKNOWN"
    GROUP BY 1, 2, 3, 4, 5

    
"""

SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=("state_code", "district", "supervision_level", "race"),
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    get_pathways_supervision_last_updated_date=state_specific_query_strings.get_pathways_supervision_last_updated_date(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    state_specific_district_filter=state_specific_query_strings.pathways_state_specific_supervision_district_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
