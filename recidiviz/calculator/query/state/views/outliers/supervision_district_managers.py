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
"""A district or regional manager, usually a supervisor of supervisors who is associated with a supervision district"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#20450): Decide on naming convention
SUPERVISION_DISTRICT_MANAGERS_VIEW_NAME = "supervision_district_managers"

SUPERVISION_DISTRICT_MANAGERS_DESCRIPTION = """A district or regional manager, usually a supervisor of supervisors who is associated with a supervision district"""


# TODO(#21877): Remove state-specific work arounds for district managers
SUPERVISION_DISTRICT_MANAGERS_QUERY_TEMPLATE = """
WITH 
us_pa_supervision_district_managers AS (
  SELECT 
    'US_PA' AS state_code,
    CAST(ext_id AS STRING) AS external_id,
    TO_JSON_STRING(
        STRUCT (
            FirstName AS given_names, 
            LastName AS surname
        )
    ) AS full_name,
    email,
    district AS supervision_district
  FROM `{project_id}.static_reference_tables.us_pa_upper_mgmt`  
  WHERE role IN ('District Director', 'Deputy District Director')
),
us_ix_supervision_district_managers AS (
  SELECT 
    state_code,
    external_id,
    TO_JSON_STRING(
        STRUCT (
            FirstName AS given_names, 
            MiddleName AS middle_names,
            Suffix AS name_suffix,  
            LastName AS surname
        )
    ) AS full_name,
    email,
    supervision_district,
    FROM `{project_id}.static_reference_tables.us_ix_state_staff_leadership`
    LEFT JOIN (
        SELECT
            state_code,
            location_external_id as LocationId,
            JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_district_id") AS supervision_district,
        FROM `{project_id}.reference_views.location_metadata_materialized`
        WHERE 
            state_code = "US_IX"
    ) districts USING (state_code, LocationId)
    WHERE role_subtype_raw_text IN ('DISTRICT MANAGER', 'DEPUTY DISTRICT MANAGER')
    AND active_flag = 1
)

SELECT 
    {columns}
FROM us_pa_supervision_district_managers 

UNION ALL

SELECT 
    {columns}
FROM us_ix_supervision_district_managers
"""

SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_DISTRICT_MANAGERS_VIEW_NAME,
    view_query_template=SUPERVISION_DISTRICT_MANAGERS_QUERY_TEMPLATE,
    description=SUPERVISION_DISTRICT_MANAGERS_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "full_name",
        "email",
        "supervision_district",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER.build_and_print()
