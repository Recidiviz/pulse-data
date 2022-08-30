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
"""Projected supervised populations"""
from datetime import date

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
from recidiviz.calculator.query.state.views.dashboard.population_projections.population_projection_time_series_query_template import (
    population_projection_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_PROJECTION_VIEW_NAME = "supervision_population_projection"

SUPERVISION_POPULATION_PROJECTION_DESCRIPTION = "Projected supervised populations"

SUPERVISION_POPULATION_PROJECTION_QUERY_TEMPLATE = f"""
    WITH projection AS (
        {population_projection_query(compartment='SUPERVISION')}
    ),
    # TODO(#14970): Remove and update once updated views have been exported
    remapped_admission_reason AS (
        SELECT legal_status AS admission_reason, *
        FROM projection
    )
    SELECT
    {{columns}}
    FROM remapped_admission_reason
"""

SUPERVISION_POPULATION_PROJECTION_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=get_pathways_supervision_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=SUPERVISION_POPULATION_PROJECTION_VIEW_NAME,
        view_query_template=SUPERVISION_POPULATION_PROJECTION_QUERY_TEMPLATE,
        description=SUPERVISION_POPULATION_PROJECTION_DESCRIPTION,
        columns=[
            "state_code",
            "year",
            "month",
            "simulation_tag",
            "gender",
            "admission_reason",
            "legal_status",
            "total_population",
            "total_population_min",
            "total_population_max",
        ],
        population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
        cur_year=str(date.today().year),
        cur_month=str(date.today().month),
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_PROJECTION_VIEW_BUILDER.build_and_print()
