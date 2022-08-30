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
#   =============================================================================
"""People who have transitioned from supervision to prison by date of reincarceration."""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_supervision_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_NAME = "supervision_to_prison_transitions"

SUPERVISION_TO_PRISON_TRANSITIONS_DESCRIPTION = (
    "Transitions from supervision to prison by month."
)

SUPERVISION_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE = """
    WITH transitions_without_unknowns AS (
        SELECT
            state_code,
            person_id,
            transition_date,
            year,
            month,
            age,
            supervising_officer,
            supervision_start_date,
            time_period,
            {dimensions_clause}
        FROM `{project_id}.{dashboard_views_dataset}.supervision_to_prison_transitions_raw`
    )
    SELECT {columns} FROM transitions_without_unknowns
"""

SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=get_pathways_supervision_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        dimensions_clause=PathwaysMetricBigQueryViewBuilder.replace_unknowns(
            [
                "supervision_type",
                "supervision_level",
                "age_group",
                "gender",
                "race",
                "supervision_district",
                "length_of_stay",
            ]
        ),
        columns=[
            "state_code",
            "person_id",
            "transition_date",
            "year",
            "month",
            "supervision_type",
            "supervision_level",
            "age",
            "age_group",
            "gender",
            "race",
            "supervising_officer",
            "supervision_start_date",
            "supervision_district",
            "time_period",
            "length_of_stay",
        ],
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_NAME,
        view_query_template=SUPERVISION_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE,
        description=SUPERVISION_TO_PRISON_TRANSITIONS_DESCRIPTION,
        dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER.build_and_print()
