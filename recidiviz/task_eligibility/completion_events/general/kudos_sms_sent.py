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
"""Kudos SMS sent thorugh the milestones dashboard for California.
"""
from recidiviz.calculator.query.state.dataset_config import (
    PULSE_DASHBOARD_SEGMENT_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.user_event_template import (
    user_event_template,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows all custody level downgrade events
for any person, across all states.
"""

_QUERY_TEMPLATE = f"""
WITH milestones_congratulations AS (
  {user_event_template(
        "frontend_milestones_congratulations_sent", 
        should_check_client_id=False, 
        should_lookup_user_from_staff_record=False
    )}
)
SELECT
    DISTINCT
    mc.state_code,
    mc.person_id,
    SAFE_CAST(mc.timestamp AS DATE) AS completion_event_date
FROM milestones_congratulations mc
"""

VIEW_BUILDER: StateAgnosticTaskCompletionEventBigQueryViewBuilder = (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder(
        completion_event_type=TaskCompletionEventType.KUDOS_SMS_SENT,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        segment_dataset=PULSE_DASHBOARD_SEGMENT_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
