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
"""
Shows the spans of time during which someone in ME is eligible
for a dishcarge from supervision
"""

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_active_supervision_and_supervision_out_of_state_population,
)
from recidiviz.task_eligibility.completion_events.general import full_term_discharge
from recidiviz.task_eligibility.criteria.general import (
    supervision_past_full_term_completion_date,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ME,
    task_name="OVERDUE_FOR_DISCHARGE_REQUEST",
    description=__doc__,
    candidate_population_view_builder=probation_active_supervision_and_supervision_out_of_state_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_past_full_term_completion_date.VIEW_BUILDER,
    ],
    completion_event_builder=full_term_discharge.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=supervision_past_full_term_completion_date.VIEW_BUILDER,
        reasons_date_field="eligible_date",
        interval_length=90,
        interval_date_part=BigQueryDateInterval.DAY,
        description="Within 90 days of full term completion date",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
