# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A parole and/or probation officer/agent with regular contact with clients."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.supervision_officers import (
    SUPERVISION_OFFICERS_QUERY_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_supervision_officers"

_DESCRIPTION = """
    A parole and/or probation officer/agent with regular contact with clients. 
    Contains an array of supervisors and a primary supervisor.
    """

# TODO(#29763): Clean up Insights vs. Outliers exports
INSIGHTS_SUPERVISION_OFFICERS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICERS_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "pseudonymized_id",
        "supervisor_external_id",
        "supervisor_external_ids",
        "supervision_district",
        "specialized_caseload_type",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_SUPERVISION_OFFICERS_VIEW_BUILDER.build_and_print()
