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
"""State agnostic view of parole board hearing decisions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_parole_board_hearing_decisions import (
    US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "parole_board_hearing_decisions"

_VIEW_DESCRIPTION = (
    "State agnostic view of parole board hearing decisions. Each row is a member's "
    "decision for a specific hearing."
)

_QUERY_TEMPLATE = """
SELECT
    *
FROM
    `{project_id}.{us_tn_pb_hearing_decisions}`
"""

PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    us_tn_pb_hearing_decisions=US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER.table_for_query.to_str(),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER.build_and_print()
