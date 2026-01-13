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
"""Shows the spans of time during which someone in NE is eligible
for good time restoration of more than 30 days.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import good_time_reinstated
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    has_received_good_time_restoration_5_consecutive_months,
)
from recidiviz.task_eligibility.eligibility_spans.us_ne.good_time_restoration_30_days import (
    US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="GOOD_TIME_RESTORATION_MORE_THAN_30_DAYS",
    description=__doc__,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=US_NE_GOOD_TIME_RESTORATION_30_DAYS_CRITERIA_BUILDERS
    + [has_received_good_time_restoration_5_consecutive_months.VIEW_BUILDER],
    completion_event_builder=good_time_reinstated.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
