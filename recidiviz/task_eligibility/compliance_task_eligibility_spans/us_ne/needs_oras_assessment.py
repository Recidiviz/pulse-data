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
"""ORAS assessment compliance task for the active NE parole population.

Combines an initial-assessment trigger (11 business days after supervision
start) and a semiannual reassessment trigger (last day of the 6th month
after the prior assessment) via an OR-group.
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_including_null_supervision_level_population,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    is_missing_semiannual_oras_assessment,
    meets_initial_oras_assessment_trigger,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

meets_oras_reassessment_or_initial_assessment_triggers = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_NE_MEETS_ORAS_REASSESSMENT_OR_INITIAL_ASSESSMENT_TRIGGERS",
        sub_criteria_list=[
            is_missing_semiannual_oras_assessment.VIEW_BUILDER,
            meets_initial_oras_assessment_trigger.VIEW_BUILDER,
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
    task_name="needs_oras_assessment",
    candidate_population_view_builder=parole_active_supervision_including_null_supervision_level_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_oras_reassessment_or_initial_assessment_triggers,
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    cadence_type=CadenceType.RECURRING_ROLLING,
    due_date_field="assessment_due_date",
    last_task_completed_date_field="most_recent_assessment_date",
    due_date_criteria_builder=meets_oras_reassessment_or_initial_assessment_triggers,
    last_task_completed_date_criteria_builder=meets_oras_reassessment_or_initial_assessment_triggers,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
