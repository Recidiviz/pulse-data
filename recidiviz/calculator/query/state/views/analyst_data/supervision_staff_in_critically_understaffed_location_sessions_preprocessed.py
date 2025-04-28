# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View that represents spans of time over which a supervision staff member
is assigned to a critically understaffed location"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.us_tx.us_tx_supervision_staff_in_critically_understaffed_location_sessions_preprocessed import (
    US_TX_SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = (
    "supervision_staff_in_critically_understaffed_location_sessions_preprocessed"
)

_QUERY_TEMPLATE = f"""
SELECT * FROM `{{project_id}}.{US_TX_SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER.table_for_query.to_str()}`
"""

SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=__doc__,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
