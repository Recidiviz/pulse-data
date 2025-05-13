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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in PA is eligible for transfer to Administrative Supervision.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_pa import (
    transfer_to_administrative_supervision,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_not_electronic_monitoring,
    supervision_level_is_not_high_or_max_for_one_year,
    supervision_level_is_not_limited,
    supervision_past_full_term_completion_date_or_upcoming_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa import (
    no_high_sanctions_in_past_year,
    not_assigned_ineligible_stat_code,
    not_on_sex_offense_protocol,
    not_serving_ineligible_offense_for_admin_supervision,
    on_supervision_at_least_one_year,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which
someone in PA is eligible for transfer to administrative supervision.
"""

not_supervision_past_full_term_completion_date_or_upcoming_90_days_view_builder = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    sub_criteria=supervision_past_full_term_completion_date_or_upcoming_90_days.VIEW_BUILDER,
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_PA,
    task_name="COMPLETE_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        on_supervision_at_least_one_year.VIEW_BUILDER,
        supervision_level_is_not_limited.VIEW_BUILDER,
        no_high_sanctions_in_past_year.VIEW_BUILDER,
        not_serving_ineligible_offense_for_admin_supervision.VIEW_BUILDER,
        not_on_sex_offense_protocol.VIEW_BUILDER,
        not_assigned_ineligible_stat_code.VIEW_BUILDER,
        not_supervision_past_full_term_completion_date_or_upcoming_90_days_view_builder,
        supervision_level_is_not_electronic_monitoring.VIEW_BUILDER,
        supervision_level_is_not_high_or_max_for_one_year.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_administrative_supervision.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=on_supervision_at_least_one_year.VIEW_BUILDER,
        reasons_date_field="minimum_time_served_date",
        interval_length=6,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 6 months of serving 1 year on parole",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
