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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
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
{staff_query_template(role="SUPERVISION_OFFICER")}
"""

SUPERVISION_OFFICERS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICERS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICERS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICERS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICERS_VIEW_BUILDER.build_and_print()
