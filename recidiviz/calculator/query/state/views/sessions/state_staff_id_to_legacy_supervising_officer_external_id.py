# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""
View that maps states to the id type associated with the legacy supervising officer external id,
derived from state_supervision_period.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#21702): Deprecate this file once legacy supervising officer external id's have been deprecated.

_VIEW_NAME = "state_staff_id_to_legacy_supervising_officer_external_id"

_VIEW_DESCRIPTION = """
View that maps state_staff staff id's to the appropriate legacy supervising officer external id,
based on the id_type used in state_supervision_period. Can be used to join tables containing legacy external id's
with new state_staff id's.
"""

_QUERY_TEMPLATE = """
# Identify the id_type that is consistent with state_supervision_periods
WITH state_to_legacy_supervising_officer_external_id_type AS (
    SELECT DISTINCT 
        state_code, 
        supervising_officer_staff_external_id_type AS id_type,
    FROM
        `{project_id}.normalized_state.state_supervision_period`
    WHERE
        supervising_officer_staff_external_id IS NOT NULL
)
SELECT
    state_code,
    staff_id,
    external_id,
FROM
    `{project_id}.normalized_state.state_staff_external_id`
INNER JOIN
    state_to_legacy_supervising_officer_external_id_type
USING
    (state_code, id_type)
-- Pick the largest (usually most recent) external_id for the small subset of staff_id's
-- having more than one external_id for a single id_type.
-- For PA, we'll exclude this staff external id that was internally constructed for easier entity merging purposes
WHERE id_type NOT IN ('US_PA_PBPP_POSNO')
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY staff_id ORDER BY external_id DESC) = 1
"""

STATE_STAFF_ID_TO_LEGACY_SUPERVISING_OFFICER_EXTERNAL_ID_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SESSIONS_DATASET,
        view_id=_VIEW_NAME,
        view_query_template=_QUERY_TEMPLATE,
        description=_VIEW_DESCRIPTION,
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_STAFF_ID_TO_LEGACY_SUPERVISING_OFFICER_EXTERNAL_ID_VIEW_BUILDER.build_and_print()
