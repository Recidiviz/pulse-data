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
"""Defines a view that shows each date someone moves from one facility to another
without a break in incarceration. Moves into, out of, and between out-of-state
facilities count as transfers.
"""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# housing_unit_sessions has rows only for INCARCERATION and INCARCERATION_OUT_OF_STATE,
# so a session counts as a transfer only when the session right before it ends the
# same day, which excludes admissions. Both facilities must be known so that a
# missing value is not read as a move.
_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date AS completion_event_date,
FROM (
    SELECT
        state_code,
        person_id,
        start_date,
        facility,
        LAG(end_date_exclusive) OVER person_window AS prev_end_date_exclusive,
        LAG(facility) OVER person_window AS prev_facility,
    FROM `{project_id}.{sessions_dataset}.housing_unit_sessions_materialized`
    WINDOW person_window AS (PARTITION BY state_code, person_id ORDER BY start_date)
)
WHERE prev_end_date_exclusive = start_date
    AND facility IS NOT NULL
    AND prev_facility IS NOT NULL
    AND facility != prev_facility
"""

VIEW_BUILDER: StateAgnosticTaskCompletionEventBigQueryViewBuilder = (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder(
        completion_event_type=TaskCompletionEventType.TRANSFER_PRISON_FACILITY,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
