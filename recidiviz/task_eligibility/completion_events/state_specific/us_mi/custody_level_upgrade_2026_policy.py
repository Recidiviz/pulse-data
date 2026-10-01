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
"""Defines a view that shows all custody level upgrade events, specifically for upgrades
happening under the new 2026 classification policy in MI.
"""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(MI-7750): This currently marks every CLU as an event under the 2026 policy,
# since MI's classification data doesn't yet distinguish which policy version a
# decision was made under (unlike TN, whose equivalent completion event additionally
# filters on assessment_type = "RCAF" to isolate 2026-policy reclassification
# decisions). Add an equivalent filter here once MI's data supports it, so this
# doesn't fire for pre-2026-policy reclassifications.
_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date AS completion_event_date,
FROM
    `{project_id}.{sessions_dataset}.custody_level_sessions_materialized`
WHERE
    state_code = "US_MI"
    AND custody_upgrade = 1
"""

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = (
    StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        completion_event_type=TaskCompletionEventType.CUSTODY_LEVEL_UPGRADE_2026_POLICY,
        description=__doc__,
        completion_event_query_template=_QUERY_TEMPLATE,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
