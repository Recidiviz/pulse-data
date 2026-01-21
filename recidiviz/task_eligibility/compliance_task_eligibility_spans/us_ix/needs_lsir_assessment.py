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
"""Defines a criteria view that shows spans of time for which supervision clients
need an LSI-R assessment. Specifically:
- Initial assessments are needed within 45 or 90 days of starting supervision
- Reassessments are needed every 365 days after the last assessment, triggered
    30 days before that date. However:
    - Clients on minimum supervision level and with GENERAL case type do not need reassessments.
    - Sex offenders with low LSI-R scores do not need reassessments.
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_ix import (
    active_supervision_population_for_tasks_without_minimum_or_xcrc,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_case_type_is_sex_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    has_low_lsir_for_reassessment,
    is_missing_annual_lsir_assessment,
    meets_initial_lsir_assessment_trigger,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# There is two task criteria wrappers here: an inverted one and a group one
# The group one is needed to combine the two criteria with AND logic
# The inverted one is needed to invert the resulting group criteria.
# This results in a criteria that is met when you are both low LSIR and
# serving for a sexual offense. This group is the one that does NOT need
# reassessments.
does_not_have_low_lsir_and_sexual_offense = (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
        sub_criteria=(
            StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
                logic_type=TaskCriteriaGroupLogicType.AND,
                criteria_name="US_IX_HAS_LOW_LSIR_AND_SEXUAL_OFFENSE",
                sub_criteria_list=[
                    has_low_lsir_for_reassessment.VIEW_BUILDER,
                    supervision_case_type_is_sex_offense.VIEW_BUILDER,
                ],
            )
        )
    )
)

# This criteria is met when the individual is missing an annual LSI-R assessment.
# It excludes SEX OFFENDERS with LOW LSIR scores
meets_lsir_reassessment_trigger = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_IX_MEETS_LSIR_REASSESSMENT_TRIGGER",
    sub_criteria_list=[
        does_not_have_low_lsir_and_sexual_offense,
        is_missing_annual_lsir_assessment.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=["case_type"],
)

# Final combine: assessment trigger criteria that combines both initial and reassessment triggers
meets_lsir_reassessment_or_initial_assessment_triggers = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_IX_MEETS_LSIR_REASSESSMENT_OR_INITIAL_ASSESSMENT_TRIGGERS",
        sub_criteria_list=[
            meets_lsir_reassessment_trigger,
            meets_initial_lsir_assessment_trigger.VIEW_BUILDER,
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
    task_name="needs_lsir_assessment",
    candidate_population_view_builder=active_supervision_population_for_tasks_without_minimum_or_xcrc.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_lsir_reassessment_or_initial_assessment_triggers,
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    due_date_field="assessment_due_date",
    due_date_criteria_builder=meets_lsir_reassessment_or_initial_assessment_triggers,
    last_task_completed_date_field="last_assessment_date",
    last_task_completed_date_criteria_builder=meets_lsir_reassessment_or_initial_assessment_triggers,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
