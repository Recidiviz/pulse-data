#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
"""View to prepare supervision staff records for Workflows for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_STAFF_RECORD_VIEW_NAME = "supervision_staff_record"

SUPERVISION_STAFF_RECORD_DESCRIPTION = """
    Supervision staff records to be exported to Firestore to power Workflows.
    """

SUPERVISION_STAFF_RECORD_QUERY_TEMPLATE = """
    WITH
        caseload_staff_external_ids AS (
            SELECT DISTINCT
                state_code,
                officer_id AS external_id,
            FROM `{project_id}.workflows_views.client_record_materialized` client
        )
        , staff_query AS (
            SELECT
                external_id AS id,
                state_code,
                district,
                email,
                given_names,
                surname,
                IF(state_code = "US_CA", role_subtype_primary, CAST(NULL AS STRING)) AS role_subtype,
                supervisor_external_id,
                supervisor_external_ids,
            FROM `{project_id}.reference_views.current_staff_materialized` current_staff
            INNER JOIN caseload_staff_external_ids ids
                USING (state_code, external_id)
        )

    SELECT {columns}
    FROM staff_query
"""

SUPERVISION_STAFF_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=SUPERVISION_STAFF_RECORD_VIEW_NAME,
    view_query_template=SUPERVISION_STAFF_RECORD_QUERY_TEMPLATE,
    description=SUPERVISION_STAFF_RECORD_DESCRIPTION,
    columns=[
        "id",
        "state_code",
        "district",
        "email",
        "given_names",
        "surname",
        "role_subtype",
        "supervisor_external_id",
        "supervisor_external_ids",
    ],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_STAFF_RECORD_VIEW_BUILDER.build_and_print()
