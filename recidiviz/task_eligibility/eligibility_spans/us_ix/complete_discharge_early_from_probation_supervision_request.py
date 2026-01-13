# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
someone in ID is eligible to request early discharge from probation supervision.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    no_felony_within_24_months,
    on_probation_at_least_one_year,
    supervision_not_past_full_term_completion_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    lsir_level_low_moderate_for_x_days,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=probation_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        on_probation_at_least_one_year.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date.VIEW_BUILDER,
        lsir_level_low_moderate_for_x_days.VIEW_BUILDER,
        no_felony_within_24_months.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=on_probation_at_least_one_year.VIEW_BUILDER,
        reasons_date_field="minimum_time_served_date",
        interval_length=3,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 3 months of probation 1 year date",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
