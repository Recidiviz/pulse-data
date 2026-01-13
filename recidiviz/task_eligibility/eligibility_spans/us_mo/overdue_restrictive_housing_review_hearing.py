# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Shows the spans of time during which someone in MO has an overdue Restrictive Housing review hearing"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mo import (
    hearing_occurred,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    hearing_after_restrictive_housing_start,
    in_restrictive_housing,
    no_d1_sanction_after_most_recent_hearing,
    past_latest_scheduled_review_date,
)
from recidiviz.task_eligibility.criteria_condition import (
    ReasonDateInCalendarWeekCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="OVERDUE_RESTRICTIVE_HOUSING_REVIEW_HEARING",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        past_latest_scheduled_review_date.VIEW_BUILDER,
        in_restrictive_housing.VIEW_BUILDER,
        hearing_after_restrictive_housing_start.VIEW_BUILDER,
        no_d1_sanction_after_most_recent_hearing.VIEW_BUILDER,
    ],
    completion_event_builder=hearing_occurred.VIEW_BUILDER,
    almost_eligible_condition=ReasonDateInCalendarWeekCriteriaCondition(
        criteria=past_latest_scheduled_review_date.VIEW_BUILDER,
        reasons_date_field="next_review_date",
        description="Next restrictive housing review hearing due this week",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
