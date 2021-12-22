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
"""Prison population snapshot by dimension."""
from recidiviz.calculator.query.bq_utils import add_age_groups, filter_to_enabled_states
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "prison_population_snapshot_by_dimension"
)

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Prison population snapshot by dimension"""
)

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH get_last_updated AS ({get_pathways_incarceration_last_updated_date}),
    add_age_groups AS (
        SELECT 
            state_code,
            gender,
            admission_reason as legal_status,
            facility,
            {add_age_groups}
            count(distinct person_id) as person_count
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_incarceration_population_metrics_included_in_state_population_materialized`
        group by state_code, gender, legal_status, facility, age_group
    )
    
    SELECT
        state_code,
        get_last_updated.last_updated,
        gender,
        legal_status,
        facility,
        age_group,
        SUM(person_count) as person_count
    FROM add_age_groups,
    UNNEST ([age_group, 'ALL']) as age_group,
    UNNEST ([legal_status, 'ALL']) as legal_status,
    UNNEST ([facility, 'ALL']) as facility,
    UNNEST ([gender, 'ALL']) as gender
    LEFT JOIN get_last_updated  USING (state_code)
    {filter_to_enabled_states}
    group by 1, 2, 3, 4, 5, 6 
"""

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=(
        "state_code",
        "last_updated",
        "gender",
        "legal_status",
        "facility",
        "age_group",
    ),
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    add_age_groups=add_age_groups(),
    get_pathways_incarceration_last_updated_date=get_pathways_incarceration_last_updated_date(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=ENABLED_STATES
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
