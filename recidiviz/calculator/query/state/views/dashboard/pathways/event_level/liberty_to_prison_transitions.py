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
"""People who have transitioned from liberty to prison by date of incarceration.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.event_level.liberty_to_prison_transitions
"""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    convert_days_to_years,
    create_buckets_with_cap,
    get_binned_time_period_months,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LIBERTY_TO_PRISON_TRANSITIONS_VIEW_NAME = "liberty_to_prison_transitions"

LIBERTY_TO_PRISON_TRANSITIONS_DESCRIPTION = (
    "Transitions from liberty to prison by month."
)

LIBERTY_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH prior_incarcerations AS (
        SELECT
            compartment.state_code,
            compartment.person_id,
            compartment.start_date AS transition_date,
            EXTRACT(YEAR FROM compartment.start_date) AS year,
            EXTRACT(MONTH FROM compartment.start_date) AS month,
            age_start AS age,
            {age_group}
            gender,
            prioritized_race_or_ethnicity AS race,
            UPPER(IFNULL(judicial_district_code_start, 'UNKNOWN')) AS judicial_district,
            IFNULL(previous_incarceration.session_length_days, 0) AS previous_incarceration_session_length_days,
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` compartment
        LEFT JOIN `{project_id}.{sessions_dataset}.incarceration_super_sessions_materialized` previous_incarceration
        ON compartment.state_code = previous_incarceration.state_code
            AND compartment.person_id = previous_incarceration.person_id
            AND compartment.session_id > previous_incarceration.session_id_end
        WHERE
            compartment.state_code IN {enabled_states}
            AND compartment.compartment_level_1 = 'INCARCERATION'
            AND compartment.inflow_from_level_1 = 'LIBERTY'
            AND compartment.start_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
            -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months
    )
    , sum_length AS (
        SELECT
            state_code,
            person_id,
            transition_date,
            SUM(previous_incarceration_session_length_days) AS prior_length_of_incarceration_days
        FROM prior_incarcerations
        group by state_code, person_id, transition_date
    )
    , all_transitions AS (
        SELECT
            prior_incarcerations.* EXCEPT (previous_incarceration_session_length_days, transition_date),
            sum_length.transition_date,
            {binned_time_periods} AS time_period,
            sum_length.prior_length_of_incarceration_days,
            {length_of_stay} AS prior_length_of_incarceration
        FROM prior_incarcerations
        LEFT JOIN sum_length
            USING(state_code, person_id, transition_date)
    )
    SELECT DISTINCT {columns} FROM all_transitions
"""

LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=get_pathways_incarceration_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=LIBERTY_TO_PRISON_TRANSITIONS_VIEW_NAME,
        view_query_template=LIBERTY_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE,
        description=LIBERTY_TO_PRISON_TRANSITIONS_DESCRIPTION,
        age_group=add_age_groups("age_start"),
        sessions_dataset=dataset_config.SESSIONS_DATASET,
        enabled_states=str(tuple(get_pathways_enabled_states())),
        binned_time_periods=get_binned_time_period_months("sum_length.transition_date"),
        length_of_stay=create_buckets_with_cap(
            convert_days_to_years("sum_length.prior_length_of_incarceration_days"), 11
        ),
        columns=[
            "transition_date",
            "year",
            "month",
            "time_period",
            "person_id",
            "age_group",
            "gender",
            "race",
            "judicial_district",
            "prior_length_of_incarceration",
            "state_code",
        ],
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER.build_and_print()
