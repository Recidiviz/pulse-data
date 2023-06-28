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
"""A upper-level management member, either state leadership or someone who oversees multiple districts"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_DIRECTORS_VIEW_NAME = "supervision_directors"

SUPERVISION_DIRECTORS_DESCRIPTION = """A upper-level management member, either state leadership or someone who oversees multiple districts"""

# TODO(#21876): Remove state-specific work arounds for state leadership
SUPERVISION_DIRECTORS_QUERY_TEMPLATE = """
WITH 
us_pa_supervision_directors AS (
  SELECT 
    'US_PA' AS state_code,
    TO_JSON_STRING(
        STRUCT (
            firstname AS given_names,  
            lastname AS surname
        )
    ) AS full_name,
    email,
  FROM `{project_id}.{static_reference_dataset}.us_pa_upper_mgmt`  
  WHERE role IN ('Executive Assistant', 'Deputy Secretary', 'Regional Director')
),
us_ix_supervision_directors AS (
  SELECT 
    state_code,
    TO_JSON_STRING(
        STRUCT (
            FirstName AS given_names,
            MiddleName AS middle_names,
            Suffix AS name_suffix,    
            LastName AS surname
        )
    ) AS full_name,
    email,
    FROM `{project_id}.{static_reference_dataset}.us_ix_state_staff_leadership`
    WHERE role_subtype_raw_text IN ('STATE LEADERSHIP')
)

SELECT 
    {columns}
FROM us_pa_supervision_directors

UNION ALL

SELECT 
    {columns}
FROM us_ix_supervision_directors
"""

SUPERVISION_DIRECTORS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_DIRECTORS_VIEW_NAME,
    view_query_template=SUPERVISION_DIRECTORS_QUERY_TEMPLATE,
    description=SUPERVISION_DIRECTORS_DESCRIPTION,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
    columns=[
        "state_code",
        "full_name",
        "email",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_DIRECTORS_VIEW_BUILDER.build_and_print()
