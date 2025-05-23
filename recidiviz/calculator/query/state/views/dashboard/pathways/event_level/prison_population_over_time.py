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
"""Prison population count time series.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_over_time
"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    get_binned_time_period_months,
)
from recidiviz.calculator.query.pathways_bq_utils import filter_to_pathways_states
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_OVER_TIME_VIEW_NAME = "prison_population_over_time"

PRISON_POPULATION_OVER_TIME_VIEW_DESCRIPTION = """Prison population count.
    This query outputs at least one row per person, per month.
    Additional rows are included for distinct attribute combinations.
"""

PRISON_POPULATION_OVER_TIME_VIEW_QUERY_TEMPLATE = """
    WITH 
    add_mapped_dimensions AS (
        SELECT 
            pop.state_code,
            date_of_stay AS date_in_population,
            {time_period_months} AS time_period,
            gender,
            sess.start_reason AS admission_reason,
            IFNULL(aggregating_location_id, pop.facility) AS facility,
            {add_age_groups}
            prioritized_race_or_ethnicity AS race,
            pop.person_id
        FROM (
            SELECT * FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_span_to_single_day_metrics_materialized` 
            WHERE included_in_state_population) pop
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_incarceration_location_name_map_materialized` name_map
            ON pop.state_code = name_map.state_code
            AND pop.facility = name_map.location_id
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` sess
            ON pop.date_of_stay BETWEEN sess.start_date AND COALESCE(sess.end_date, CURRENT_DATE('US/Eastern'))
            AND pop.person_id = sess.person_id
        WHERE date_of_stay >= DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH)
        AND EXTRACT(DAY FROM date_of_stay) = 1
    )
    , filtered_rows AS (
        SELECT 
            state_code,
            date_in_population,
            time_period,
            person_id,
            {dimensions_clause},
        FROM add_mapped_dimensions
        {filter_to_enabled_states}
        AND {facility_filter} AND {inferred_period_filter}
        AND date_in_population <= CURRENT_DATE('US/Eastern')
        AND time_period IS NOT NULL
    )
    SELECT DISTINCT
        {columns}
    FROM filtered_rows
"""

PRISON_POPULATION_OVER_TIME_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=state_specific_query_strings.get_pathways_incarceration_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        columns=[
            # state_code needs to appear first here to support remapping state code values during the ATLAS migration
            "state_code",
            "date_in_population",
            "time_period",
            "person_id",
            "age_group",
            "facility",
            "gender",
            "admission_reason",
            "race",
        ],
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=PRISON_POPULATION_OVER_TIME_VIEW_NAME,
        view_query_template=PRISON_POPULATION_OVER_TIME_VIEW_QUERY_TEMPLATE,
        description=PRISON_POPULATION_OVER_TIME_VIEW_DESCRIPTION,
        dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
        materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
        add_age_groups=add_age_groups(),
        filter_to_enabled_states=filter_to_pathways_states(
            state_code_column="state_code"
        ),
        dimensions_clause=PathwaysMetricBigQueryViewBuilder.replace_unknowns(
            [
                "gender",
                "admission_reason",
                "facility",
                "age_group",
                "race",
            ]
        ),
        facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
        inferred_period_filter=state_specific_query_strings.state_specific_facility_type_inclusion_filter(),
        time_period_months=get_binned_time_period_months(
            "date_of_stay",
            current_date_expr="DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH)",
        ),
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_OVER_TIME_VIEW_BUILDER.build_and_print()
