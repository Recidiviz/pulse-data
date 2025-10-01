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
Defines a criteria span view that shows spans of time during which a client
requires a scheduled home contact, taking into account both standard and
critical understaffing policies.
"""

from recidiviz.calculator.query.state.views.workflows.firestore.task_config import (
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    prioritized_supervision_population,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    needs_unscheduled_home_contact_monthly_critical_understaffing,
    needs_unscheduled_home_contact_standard_policy,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TX_NEEDS_UNSCHEDULED_HOME_CONTACT_CRITERIA_BUILDER = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_TX_NEEDS_UNSCHEDULED_HOME_CONTACT",
        sub_criteria_list=[
            needs_unscheduled_home_contact_standard_policy.VIEW_BUILDER,
            needs_unscheduled_home_contact_monthly_critical_understaffing.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[
            "frequency",
            "type_of_contact",
            "contact_count",
            "contact_due_date",
            "last_contact_date",
            "contact_type",
            "overdue_flag",
            "period_type",
            "contact_cadence",
        ],
    )
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="needs_unscheduled_home_contact",
    candidate_population_view_builder=prioritized_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        US_TX_NEEDS_UNSCHEDULED_HOME_CONTACT_CRITERIA_BUILDER
    ],
    compliance_type=ComplianceType.CONTACT,
    due_date_field="contact_due_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
