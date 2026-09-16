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
"""Defines a view that shows when intake classification hearings have occurred that 
use the new 2026 classification policy.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_completion_event_builders import (
    intake_classification_decision_completed_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#61946): Deprecate this completion event in favor of combining all diagnostic intake
# transfers into a single completion event in TN.
VIEW_BUILDER: StateSpecificTaskCompletionEventBigQueryViewBuilder = intake_classification_decision_completed_view_builder(
    state_code=StateCode.US_TN,
    description=__doc__,
    # Filters to the 2026 diagnostic CAF, TN's intake classification instrument.
    additional_where_clause="c.assessment_type = 'DCAF'",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
