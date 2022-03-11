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
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    DASHBOARD_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_supervision_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
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
    , all_dimensions AS (
        SELECT 
            state_code, 
            last_updated,
            IFNULL(location_name, location_id) AS district,
            CASE 
                WHEN state_code='US_ID' and COALESCE(supervision_level, "INTERNAL_UNKNOWN") = "INTERNAL_UNKNOWN"
                    THEN "OTHER"
                ELSE IFNULL(supervision_level, "EXTERNAL_UNKNOWN")
            END as supervision_level,
            COUNT(DISTINCT person_id) as person_count,
        FROM `{project_id}.{metrics_dataset}.most_recent_single_day_supervision_population_metrics_materialized`,
        UNNEST([level_1_supervision_location_external_id, 'ALL']) AS location_id,
        UNNEST([supervision_level, 'ALL']) AS supervision_level
        LEFT JOIN get_last_updated  USING (state_code)
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` USING (state_code, location_id)
        {filter_to_enabled_states}
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        state_code, 
        last_updated,
        district,
        supervision_level,
        person_count,
    FROM all_dimensions
    WHERE supervision_level IS NOT NULL
        AND supervision_level != "EXTERNAL_UNKNOWN"

    
"""

SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=(
        "state_code",
        "district",
        "supervision_level",
    ),
    dashboards_dataset=DASHBOARD_VIEWS_DATASET,
    metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    get_pathways_supervision_last_updated_date=get_pathways_supervision_last_updated_date(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=ENABLED_STATES
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
