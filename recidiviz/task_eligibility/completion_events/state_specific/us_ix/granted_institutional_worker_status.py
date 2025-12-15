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
"""Defines a view that shows transfers to Community Reentry Centers (CRC)
in ID, restricted to transfers when the resident were not eligible for work release."""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    IX_CRC_FACILITIES,
    ix_crc_facilities_in_location_sessions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""WITH crc_spans AS (
    {ix_crc_facilities_in_location_sessions(
            crc_facilities_list=IX_CRC_FACILITIES)}
)
SELECT 
    crc_admission.state_code,
    crc_admission.person_id,
    crc_admission.start_date AS completion_event_date,
FROM (
    {aggregate_adjacent_spans(table_name="crc_spans")}
) crc_admission
LEFT JOIN
    `{{project_id}}.task_eligibility_criteria_us_ix.crc_work_release_time_based_criteria_materialized` wr_criteria
ON
    crc_admission.state_code = wr_criteria.state_code
    AND crc_admission.person_id = wr_criteria.person_id
    AND crc_admission.start_date BETWEEN wr_criteria.start_date AND {nonnull_end_date_exclusive_clause("wr_criteria.end_date")}
# TODO(#24043) - only keep events when someone is not transferred for work-release
# ATLAS does not yet track that, use the work release time served criteria as a proxy for now
WHERE
    NOT IFNULL(wr_criteria.meets_criteria, FALSE)
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    completion_event_type=TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
