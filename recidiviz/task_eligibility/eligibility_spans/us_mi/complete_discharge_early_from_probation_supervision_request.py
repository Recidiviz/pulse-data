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
someone in MI is eligible for early discharge from probation supervision.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_active_supervision_and_supervision_out_of_state_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    custodial_authority_is_supervision_authority_or_other_state,
    supervision_not_past_full_term_completion_date_or_upcoming_30_days,
    supervision_or_supervision_out_of_state_past_half_full_term_release_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    no_active_ppo,
    no_new_ineligible_offenses_for_early_discharge_from_supervision,
    not_serving_ineligible_offenses_for_early_discharge_from_probation_supervision,
    supervision_is_not_ic_in,
    supervision_level_is_not_modified,
    supervision_status_is_not_delayed_sentence,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in MI is eligible
to request early discharge from probation supervision.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=probation_active_supervision_and_supervision_out_of_state_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        not_serving_ineligible_offenses_for_early_discharge_from_probation_supervision.VIEW_BUILDER,
        no_active_ppo.VIEW_BUILDER,
        no_new_ineligible_offenses_for_early_discharge_from_supervision.VIEW_BUILDER,
        supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date_or_upcoming_30_days.VIEW_BUILDER,
        supervision_is_not_ic_in.VIEW_BUILDER,
        custodial_authority_is_supervision_authority_or_other_state.VIEW_BUILDER,
        supervision_status_is_not_delayed_sentence.VIEW_BUILDER,
        supervision_level_is_not_modified.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
        reasons_date_field="half_full_term_release_date",
        interval_length=30,
        interval_date_part=BigQueryDateInterval.DAY,
        description="Within 30 days of completing half their full term supervision or supervision out of state sentence",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
