# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A supervision unit is a group of officers associated with a common manager officer, whose parent is a supervision district."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.location_query_template import (
    location_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_UNITS_VIEW_NAME = "supervision_units"

SUPERVISION_UNITS_DESCRIPTION = """A supervision unit is a group of officers associated with a common manager officer, whose parent is a supervision district."""


SUPERVISION_UNITS_QUERY_TEMPLATE = f"""
WITH
supervision_units AS (
    {location_query_template(location="supervision_unit", parent_id="supervision_district_id")}
)

SELECT 
    {{columns}}
FROM supervision_units
"""

SUPERVISION_UNITS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_UNITS_VIEW_NAME,
    view_query_template=SUPERVISION_UNITS_QUERY_TEMPLATE,
    description=SUPERVISION_UNITS_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    should_materialize=True,
    columns=["state_code", "external_id", "name", "supervision_district_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_UNITS_VIEW_BUILDER.build_and_print()
