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
"""Shows the spans of time during which someone in MO has an overdue Restrictive Housing
initial hearing.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mo import (
    hearing_occurred,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    in_restrictive_housing,
    initial_hearing_past_due_date,
    no_hearing_after_restrictive_housing_start,
    not_progressive_discipline_sanction_after_restrictive_housing_start,
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
    task_name="OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        initial_hearing_past_due_date.VIEW_BUILDER,
        in_restrictive_housing.VIEW_BUILDER,
        no_hearing_after_restrictive_housing_start.VIEW_BUILDER,
        not_progressive_discipline_sanction_after_restrictive_housing_start.VIEW_BUILDER,
    ],
    completion_event_builder=hearing_occurred.VIEW_BUILDER,
    almost_eligible_condition=ReasonDateInCalendarWeekCriteriaCondition(
        criteria=initial_hearing_past_due_date.VIEW_BUILDER,
        reasons_date_field="next_review_date",
        description="Initial restrictive housing review review due this week",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
