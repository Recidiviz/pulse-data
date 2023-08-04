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
"""A parole and/or probation officer/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.staff_query_template import (
    staff_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME = "supervision_officer_supervisors"

SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION = """A parole and/or probation officer/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload"""


SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE = f"""
WITH
supervision_officer_supervisors AS (
    {staff_query_template(role="SUPERVISION_OFFICER_SUPERVISOR")}
),
us_ix_additional_supervisors AS (
    -- Include additional staff who do not have role_subtype=SUPERVISION_OFFICER_SUPERVISOR but directly supervise officers
    SELECT *
    FROM `{{project_id}}.{{reference_views_dataset}}.us_ix_leadership_supervisors_materialized`
),
us_ix_supervision_officer_supervisors AS (
    SELECT
        {{columns}}
    FROM supervision_officer_supervisors
    WHERE state_code = 'US_IX'

    UNION ALL

    SELECT
        {{columns}}
    FROM us_ix_additional_supervisors
),
-- PA and ID are currently being joined separately because PA state staff is not hydrating
-- emails correctly, so we need to pull emails from raw data in PA only. This logic
-- will be simplified after a change to ingest logic for state_staff  TODO(#22842)

-- We are also joining on the DM product view because the district numbers for some 
-- supervisors who are also District Managers were missing. This will be fixed by a later
-- ingest change.
us_pa_supervision_officer_supervisors AS (
    SELECT DISTINCT
        supervision_officer_supervisors.state_code,
        external_id,
        staff_id,
        supervision_officer_supervisors.full_name,
        COALESCE(agent_roster.email_address, dm_product_view.email) AS email,
        supervisor_external_id,
        COALESCE(supervision_officer_supervisors.supervision_district, dm_product_view.supervision_district) AS supervision_district,
    FROM supervision_officer_supervisors
    LEFT JOIN `{{project_id}}.{{outliers_dataset}}.supervision_district_managers_materialized` dm_product_view
    USING(external_id)
    LEFT JOIN `{{project_id}}.{{raw_dataset}}.RECIDIVIZ_REFERENCE_agent_districts_latest` agent_roster
    ON(supervision_officer_supervisors.external_id = agent_roster.Employ_Num) 
    WHERE supervision_officer_supervisors.state_code = 'US_PA'
)

SELECT * FROM us_pa_supervision_officer_supervisors
WHERE supervision_district NOT IN ('FAST','CO')


UNION ALL

SELECT * FROM us_ix_supervision_officer_supervisors
"""

SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    raw_dataset=dataset_config.US_PA_RAW_DATASET,
    outliers_dataset=dataset_config.OUTLIERS_VIEWS_DATASET,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "email",
        "supervisor_external_id",
        "supervision_district",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()
