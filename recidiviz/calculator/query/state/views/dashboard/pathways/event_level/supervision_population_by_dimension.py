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
"""Supervision population count time series.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population
"""
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
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_BY_DIMENSION_VIEW_NAME = "supervision_population_by_dimension"

SUPERVISION_POPULATION_BY_DIMENSION_VIEW_DESCRIPTION = """Supervision population count.
    This query outputs one row per session, per month.
"""

SUPERVISION_POPULATION_BY_DIMENSION_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT DISTINCT {columns}
    FROM `{project_id}.{dashboards_dataset}.supervision_population`
    WHERE end_date IS NULL
    ORDER BY state_code, person_id
"""

SUPERVISION_POPULATION_BY_DIMENSION_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=get_pathways_supervision_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        columns=[
            "state_code",
            "person_id",
            "supervision_district",
            "supervision_level",
            "race",
        ],
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=SUPERVISION_POPULATION_BY_DIMENSION_VIEW_NAME,
        view_query_template=SUPERVISION_POPULATION_BY_DIMENSION_VIEW_QUERY_TEMPLATE,
        description=SUPERVISION_POPULATION_BY_DIMENSION_VIEW_DESCRIPTION,
        dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_DIMENSION_VIEW_BUILDER.build_and_print()
