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
"""Early release events from incarceration that are not considered overdue (released on or before eligible date)"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    early_release_completion_event_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = StateSpecificTaskCompletionEventBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    completion_event_type=TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE,
    description=__doc__,
    completion_event_query_template=early_release_completion_event_query_template(
        release_type="TPR",
        release_is_overdue=False,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
