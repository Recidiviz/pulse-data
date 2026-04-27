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
"""Defines a compliance TES view that shows spans of time for which supervision
clients do not meet edge case home contact standards. Combines three separate
criteria via OR logic:
  - Initial home contact (30 days after initial non-HOME office contact)
  - Address change home contact (30 days after address change)
  - Return from custody home contact (5 days after return from custody)
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    prioritized_supervision_population_not_in_custody_or_warrant_with_officer,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    meets_address_change_home_contact_trigger,
    meets_initial_home_contact_trigger,
    meets_return_from_custody_home_contact_trigger,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TX_NEEDS_EDGE_CASE_HOME_CONTACT_CRITERIA_GROUP = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_TX_NEEDS_EDGE_CASE_HOME_CONTACT",
        sub_criteria_list=[
            meets_initial_home_contact_trigger.VIEW_BUILDER,
            meets_address_change_home_contact_trigger.VIEW_BUILDER,
            meets_return_from_custody_home_contact_trigger.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[
            "contact_cadence",
            "contact_count",
            "contact_due_date",
            "last_contact_date",
            "overdue_flag",
            "causal_date",
            "criteria_name",
        ],
        reasons_aggregate_function_override={
            "criteria_name": "STRING_AGG",
            "contact_cadence": "STRING_AGG",
            "overdue_flag": "LOGICAL_OR",
        },
        reasons_aggregate_function_use_ordering_clause={
            "criteria_name",
            "contact_cadence",
        },
    )
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="needs_edge_case_home_contact_standards",
    candidate_population_view_builder=prioritized_supervision_population_not_in_custody_or_warrant_with_officer.VIEW_BUILDER,
    criteria_spans_view_builders=[
        US_TX_NEEDS_EDGE_CASE_HOME_CONTACT_CRITERIA_GROUP,
    ],
    compliance_type=ComplianceType.CONTACT,
    cadence_type=CadenceType.RECURRING_FIXED,
    due_date_field="contact_due_date",
    last_task_completed_date_field="last_contact_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
