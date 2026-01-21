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
"""
Shows the spans of time during which someone in NC is eligible for a Credit Reduction
Review (CRR).
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    community_confinement_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_nc import (
    granted_supervision_sentence_reduction,
)
from recidiviz.task_eligibility.criteria.general import (
    continuous_employment_for_90_days,
    continuous_student_for_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nc import (
    completion_of_facility_program_during_prs,
    continuous_enrollment_at_facility_for_90_days,
    no_pending_violations_or_convictions_precluding_crr,
    reporting_as_directed,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NC,
    task_name="CREDIT_REDUCTION_REVIEW",
    description=__doc__,
    candidate_population_view_builder=community_confinement_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_NC_90_CONSECUTIVE_DAYS_OF_POSITIVE_BEHAVIOR_FOR_CRR",
            sub_criteria_list=[
                # 1. 90 consecutive days of active enrollment in an education program OR
                continuous_student_for_90_days.VIEW_BUILDER,
                # 2. 90 consecutive days of employment, demonstrated by proof of wages OR
                continuous_employment_for_90_days.VIEW_BUILDER,
                # 3. 90 consecutive days at a facility or institution for medical or
                #       psychological treatment or facility providing rehabilitation,
                #       instruction, recreation, or residence  OR
                continuous_enrollment_at_facility_for_90_days.VIEW_BUILDER,
                completion_of_facility_program_during_prs.VIEW_BUILDER,
            ],
            reasons_aggregate_function_override={"employment_status": "STRING_AGG"},
            reasons_aggregate_function_use_ordering_clause={"employment_status"},
            allowed_duplicate_reasons_keys=["employment_status"],
        ),
        # Stub criteria that always return True - can be toggled via admin panel
        no_pending_violations_or_convictions_precluding_crr.VIEW_BUILDER,
        reporting_as_directed.VIEW_BUILDER,
    ],
    # TODO(#54787): Hydrate completion event
    completion_event_builder=granted_supervision_sentence_reduction.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
