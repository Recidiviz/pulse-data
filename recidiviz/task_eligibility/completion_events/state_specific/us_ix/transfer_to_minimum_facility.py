# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Defines a view that shows transfers to a minimum facility (CRC-like bed) in ID.
Uses the ind_OffenderInternalStatus table to identify when someone is assigned
work release status."""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
SELECT
    DISTINCT 
        peid.state_code,
        peid.person_id,
        SAFE_CAST(LEFT(ois.EffectiveDate, 10) AS DATE) AS completion_event_date,
FROM `{project_id}.us_ix_raw_data_up_to_date_views.ind_OffenderInternalStatus_latest` ois
INNER JOIN `{project_id}.us_ix_raw_data_up_to_date_views.ind_InternalStatus_latest` ist
    ON ois.InternalStatusId = ist.InternalStatusId
INNER JOIN `{project_id}.us_ix_normalized_state.state_person_external_id` peid
    ON peid.external_id = ois.OffenderId
    AND peid.state_code = 'US_IX'
    AND peid.id_type = 'US_IX_DOC'
WHERE ist.InternalStatusDesc = 'Work Release'
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        completion_event_type=TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
