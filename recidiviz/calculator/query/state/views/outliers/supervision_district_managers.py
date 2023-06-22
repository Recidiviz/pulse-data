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
from recidiviz.calculator.query.state.views.outliers.staff_query_template import (
    staff_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#20450): Decide on naming convention
SUPERVISION_DISTRICT_MANAGERS_VIEW_NAME = "supervision_district_managers"

SUPERVISION_DISTRICT_MANAGERS_DESCRIPTION = """A district or regional manager, usually a supervisor of supervisors who is associated with a supervision district"""


SUPERVISION_DISTRICT_MANAGERS_QUERY_TEMPLATE = f"""
WITH 
supervision_district_managers AS (
    {staff_query_template(role="SUPERVISION_REGIONAL_MANAGER")}
),
us_pa_supervision_district_managers AS (
  SELECT 
    'US_PA' AS state_code,
    CAST(ext_id AS STRING) AS external_id,
    TRIM(CONCAT(COALESCE(firstname, ''), ' ', COALESCE(lastname, ''))) AS full_name,
    email,
    district AS supervision_district
  FROM `{{project_id}}.{{static_reference_dataset}}.us_pa_upper_mgmt`  
  WHERE role IN ('District Director', 'Deputy District Director')
),
us_ix_supervision_district_managers AS (
  SELECT 
    state_code,
    external_id,
    TRIM(CONCAT(COALESCE(FirstName, ''), ' ', COALESCE(LastName, ''))) AS full_name,
    email,
    LocationId AS supervision_district,
    FROM `{{project_id}}.{{static_reference_dataset}}.us_ix_state_staff_leadership`
    WHERE role_subtype_raw_text IN ('DISTRICT MANAGER', 'DEPUTY DISTRICT MANAGER')
)

SELECT 
    {{columns}}
FROM us_pa_supervision_district_managers 

UNION ALL

SELECT 
    {{columns}}
FROM us_ix_supervision_district_managers  

UNION ALL

SELECT 
    {{columns}}
FROM supervision_district_managers
"""

SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_DISTRICT_MANAGERS_VIEW_NAME,
    view_query_template=SUPERVISION_DISTRICT_MANAGERS_QUERY_TEMPLATE,
    description=SUPERVISION_DISTRICT_MANAGERS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
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
