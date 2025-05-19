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
"""Defines a view that shows all transfers to institutional workers status for any person,
across all states.
"""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = f"""
-- Identify all spans when someone is in 309. These could include adjacent spans where
-- someone is in 309 in two different locations
WITH in_309_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        in_309
    FROM
        `{{project_id}}.sessions.us_ar_non_traditional_bed_sessions_preprocessed_materialized`
    WHERE in_309
)
,
-- Aggregate adjacent 309 spans to find when someone was transferred to 309 from somewhere
-- else.
in_309_spans_collapsed AS (
    {aggregate_adjacent_spans(
        table_name="in_309_spans",
        index_columns=["state_code", "person_id"],
        attribute=["in_309"],
        end_date_field_name='end_date_exclusive'
    )}
)
SELECT
    state_code,
    person_id,
    start_date AS completion_event_date,
FROM in_309_spans_collapsed
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_AR,
    completion_event_type=TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
