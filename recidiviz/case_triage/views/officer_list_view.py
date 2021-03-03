# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates the view builder and view for listing all known officers."""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFICER_LIST_QUERY_TEMPLATE = """
WITH names AS (
SELECT
    state_code,
    external_id,
    -- TODO(#5445): We are currently abusing MAX here to give us a single given_name/surname
    -- for each combination. When we have a reliable upstream data source for names for state_agents
    -- we should replace this.
    MAX(given_names) AS given_names,
    MAX(surname) AS surname
FROM
    `{project_id}.{reference_views_dataset}.augmented_agent_info`
WHERE
    agent_type = 'SUPERVISION_OFFICER'
GROUP BY state_code, external_id
),
rename_columns AS (
SELECT
    state_code,
    officer_external_id AS external_id,
    email_address
FROM
    `{project_id}.{static_reference_dataset}.po_report_recipients`
)
SELECT
    {columns}
FROM
    names
INNER JOIN
    rename_columns
USING (state_code, external_id)
"""

OFFICER_LIST_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="etl_officers",
    view_query_template=OFFICER_LIST_QUERY_TEMPLATE,
    columns=[
        "state_code",
        "external_id",
        "email_address",
        "given_names",
        "surname",
    ],
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_LIST_VIEW_BUILDER.build_and_print()
