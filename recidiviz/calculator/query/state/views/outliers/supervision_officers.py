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
"""A parole and/or probation officer/agent with regular contact with clients"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.staff_query_template import (
    staff_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICERS_VIEW_NAME = "supervision_officers"

SUPERVISION_OFFICERS_DESCRIPTION = (
    """A parole and/or probation officer/agent with regular contact with clients"""
)


SUPERVISION_OFFICERS_QUERY_TEMPLATE = f"""
WITH 
supervision_officers AS (
    {staff_query_template(role="SUPERVISION_OFFICER")}
),
us_ix_additional_officers AS (
    -- Include additional staff who do not have role_subtype=SUPERVISION_OFFICER but do have caseloads
    SELECT *
    FROM `{{project_id}}.{{reference_views_dataset}}.us_ix_dual_supervisors_officers_materialized`
)

SELECT 
    {{columns}}
FROM supervision_officers

UNION ALL

SELECT
    {{columns}}
FROM us_ix_additional_officers
"""

SUPERVISION_OFFICERS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICERS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICERS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICERS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "email",
        "supervisor_external_id",
        "supervision_district",
        "supervision_unit",
        "specialized_caseload_type",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICERS_VIEW_BUILDER.build_and_print()
