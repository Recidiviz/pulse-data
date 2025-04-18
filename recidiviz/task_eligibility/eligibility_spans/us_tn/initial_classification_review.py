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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in TN is eligible for an initial classification.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population_state_prison,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    incarceration_assessment_completed,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_compared_to_recommended,
    custody_level_is_not_max,
    has_initial_classification_in_state_prison_custody,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Builder for a task eligibility spans view that shows the spans of time during which
someone in TN is eligible for an initial classification.
"""

NOT_INITIAL_CLASSIFICATION_SINCE_STATE_PRISON_CUSTODY_START = (
    InvertedTaskCriteriaBigQueryViewBuilder(
        sub_criteria=has_initial_classification_in_state_prison_custody.VIEW_BUILDER,
    )
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="INITIAL_CLASSIFICATION_REVIEW",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population_state_prison.VIEW_BUILDER,
    criteria_spans_view_builders=[
        NOT_INITIAL_CLASSIFICATION_SINCE_STATE_PRISON_CUSTODY_START,
        custody_level_is_not_max.VIEW_BUILDER,
        # This criteria is used to add the current and recommended custody levels into the reasons blob for easier
        # access to the fields on the front end. This could also be done via the opportunity record query but doing it
        # this way standardizes across all TN facilities opportunities related to classification, including custody
        # level downgrade. For initial classification, everyone with a span meets the criteria
        custody_level_compared_to_recommended.VIEW_BUILDER,
    ],
    completion_event_builder=incarceration_assessment_completed.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
