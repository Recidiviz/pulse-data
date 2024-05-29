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
)

SELECT 
    {{columns}}
FROM supervision_officer_supervisors
"""

SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "pseudonymized_id",
        "supervision_district",
        "supervision_unit",
        "email",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()
