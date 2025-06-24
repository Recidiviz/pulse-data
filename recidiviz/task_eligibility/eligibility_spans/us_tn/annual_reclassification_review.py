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
someone in TN is eligible for an annual reclassification.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population_state_prison_exclude_safekeeping,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    incarceration_assessment_completed,
)
from recidiviz.task_eligibility.criteria.general import (
    custody_level_compared_to_recommended,
    custody_level_is_not_max,
    has_initial_classification_in_state_prison_custody,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    at_least_12_months_since_latest_assessment,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Builder for a task eligibility spans view that shows the spans of time during which
someone in TN is eligible for an annual reclassification.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="ANNUAL_RECLASSIFICATION_REVIEW",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population_state_prison_exclude_safekeeping.VIEW_BUILDER,
    criteria_spans_view_builders=[
        at_least_12_months_since_latest_assessment.VIEW_BUILDER,
        has_initial_classification_in_state_prison_custody.VIEW_BUILDER,
        custody_level_is_not_max.VIEW_BUILDER,
        # This criteria is used to add the current and recommended custody levels into the reasons blob for easier
        # access to the fields on the front end. This could also be done via the opportunity record query but doing it
        # this way standardizes across all TN facilities opportunities related to classification, including custody
        # level downgrade. For annual reclassification, everyone with a span meets the criteria
        custody_level_compared_to_recommended.VIEW_BUILDER,
    ],
    completion_event_builder=incarceration_assessment_completed.VIEW_BUILDER,
    # Almost eligible population includes clients in the week before the
    # in which their assessment is due, as well as during the month in which
    # the assessment is due.
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=at_least_12_months_since_latest_assessment.VIEW_BUILDER,
        reasons_date_field="assessment_due_month",
        interval_length=1,
        interval_date_part=BigQueryDateInterval.WEEK,
        description="Within 1 week of month during which assessment is due",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
