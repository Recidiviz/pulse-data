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
    incarceration_population_state_prison_exclude_safekeeping,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    incarceration_intake_assessment_completed,
)
from recidiviz.task_eligibility.criteria.general import custody_level_is_not_max
from recidiviz.task_eligibility.eligibility_spans.us_tn.initial_classification_review_2026_policy import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_CRITERIA_VIEW_BUILDERS,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="INITIAL_CLASSIFICATION_REVIEW",
    description=__doc__,
    candidate_population_view_builder=incarceration_population_state_prison_exclude_safekeeping.VIEW_BUILDER,
    criteria_spans_view_builders=[
        *US_TN_INITIAL_CLASSIFICATION_REVIEW_CRITERIA_VIEW_BUILDERS,
        custody_level_is_not_max.VIEW_BUILDER,
    ],
    completion_event_builder=incarceration_intake_assessment_completed.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
