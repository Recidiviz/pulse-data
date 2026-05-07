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
# =============================================================================
"""STABLE assessment compliance task for NE male sex-offender parolees.

Combines an initial-assessment trigger (30 days after the SO supervision
episode begins, with a 40-day pre-start lookback for already-completed
assessments) and an annual reassessment trigger (last day of the 12th month
after the prior assessment) via an OR-group. Male / SO / parole filtering is
handled by the candidate population.
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_ne import (
    active_male_sex_offender_supervision_population_for_tasks,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.general import on_parole_at_least_10_years
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    is_missing_annual_stable_assessment,
    meets_initial_stable_assessment_trigger,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

meets_stable_reassessment_or_initial_assessment_triggers = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_NE_MEETS_STABLE_REASSESSMENT_OR_INITIAL_ASSESSMENT_TRIGGERS",
        sub_criteria_list=[
            is_missing_annual_stable_assessment.VIEW_BUILDER,
            meets_initial_stable_assessment_trigger.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[
            "assessment_due_date",
            "most_recent_assessment_date",
        ],
        reasons_aggregate_function_override={
            "assessment_due_date": "MIN",
            "most_recent_assessment_date": "MAX",
        },
    )
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="needs_stable_assessment",
    candidate_population_view_builder=active_male_sex_offender_supervision_population_for_tasks.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_stable_reassessment_or_initial_assessment_triggers,
        StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=on_parole_at_least_10_years.VIEW_BUILDER
        ),
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    cadence_type=CadenceType.RECURRING_ROLLING,
    due_date_field="assessment_due_date",
    last_task_completed_date_field="most_recent_assessment_date",
    due_date_criteria_builder=meets_stable_reassessment_or_initial_assessment_triggers,
    last_task_completed_date_criteria_builder=meets_stable_reassessment_or_initial_assessment_triggers,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
