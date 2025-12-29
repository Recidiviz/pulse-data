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
"""Defines a criteria view that shows spans of time for which sex offense supervision clients
need a STABLE assessment. Specifically:
- Initial assessments are needed within 90 days of starting supervision for sex offenders
- Reassessments are needed every 365 days after the last assessment, triggered
    30 days before that date.
- Unlike LSI-R assessments, even clients on minimum supervision level need STABLE reassessments.
- This only applies to sex offenders (handled by the candidate population).
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_ix import (
    active_male_sex_offender_supervision_population_for_tasks,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    is_missing_annual_stable_assessment,
    meets_initial_stable_assessment_trigger,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Combine initial and reassessment triggers.
# Unlike LSI-R, even MINIMUM level clients need STABLE reassessments.
# Sex offender filtering is handled by the candidate population.
meets_stable_reassessment_or_initial_assessment_triggers = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_IX_MEETS_STABLE_REASSESSMENT_OR_INITIAL_ASSESSMENT_TRIGGERS",
        sub_criteria_list=[
            is_missing_annual_stable_assessment.VIEW_BUILDER,
            meets_initial_stable_assessment_trigger.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[
            "assessment_due_date",
            "last_assessment_date",
            "contact_cadence",
        ],
        reasons_aggregate_function_override={
            "assessment_due_date": "MIN",
            "last_assessment_date": "MAX",
        },
    )
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="needs_stable_assessment",
    candidate_population_view_builder=active_male_sex_offender_supervision_population_for_tasks.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_stable_reassessment_or_initial_assessment_triggers,
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    due_date_field="assessment_due_date",
    due_date_criteria_builder=meets_stable_reassessment_or_initial_assessment_triggers,
    last_task_completed_date_field="last_assessment_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
