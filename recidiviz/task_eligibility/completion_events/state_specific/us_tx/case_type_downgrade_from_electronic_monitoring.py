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
"""Defines a view that shows downgrades from the electronic monitoring case type for parole clients in Texas."""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_QUERY_TEMPLATE = """
-- TODO(OBT-45606): This criterion uses the Tasks view case_type_supervision_level_spans_materialized because Texas's case_type only gets properly configured at that stage. If case_type gets configured earlier on in ingest, then change the source for this criterion to be whichever view is most upstream and encodes case_type correctly. 
WITH ordered_spans AS (
    SELECT 
        person_id,
        state_code,
        case_type,
        start_date,
        end_date,
        LEAD(case_type) OVER (PARTITION BY person_id, state_code ORDER BY start_date) AS next_case_type,
        LEAD(start_date) OVER (PARTITION BY person_id, state_code ORDER BY start_date) AS next_start_date,
    FROM `{project_id}.{tasks_dataset}.case_type_supervision_level_spans_materialized`
    WHERE state_code = 'US_TX'
)
SELECT
    state_code,
    person_id,
    end_date AS completion_event_date,
FROM ordered_spans
WHERE case_type = 'ELECTRONIC_MONITORING'
    AND end_date IS NOT NULL
    AND (
        -- Case type changed to something else, immediately (no gap in the data)
        (next_case_type NOT IN ('ELECTRONIC_MONITORING', 'INTENSE_SUPERVISION') AND next_start_date = end_date)
        -- Or the EM span closed with nothing after it -- i.e. left supervision entirely
        OR next_case_type IS NULL
    )
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    completion_event_type=TaskCompletionEventType.CASE_TYPE_DOWNGRADE_FROM_ELECTRONIC_MONITORING,
    description=__doc__,
    completion_event_query_template=_QUERY_TEMPLATE,
    tasks_dataset=dataset_config.TASKS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
