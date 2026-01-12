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
need a home visit (i.e. are not compliant with scheduled home visits)
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_ix import (
    active_supervision_population_for_tasks,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    meets_address_changes_triggers,
    meets_home_visit_triggers,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

meets_home_visit_or_address_changes_triggers = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_IX_MEETS_HOME_VISIT_OR_ADDRESS_CHANGES_TRIGGERS",
        sub_criteria_list=[
            meets_home_visit_triggers.VIEW_BUILDER,
            meets_address_changes_triggers.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=["contact_due_date", "last_contact_date"],
        reasons_aggregate_function_override={
            "contact_due_date": "MIN",
            "last_contact_date": "MAX",
        },
    )
)
# TODO(#51098): Change candidate population so it filters to relevant case types and supervision levels
VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="needs_home_visit",
    candidate_population_view_builder=active_supervision_population_for_tasks.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_home_visit_or_address_changes_triggers,
    ],
    compliance_type=ComplianceType.CONTACT,
    due_date_criteria_builder=meets_home_visit_or_address_changes_triggers,
    due_date_field="contact_due_date",
    last_task_completed_date_field="last_contact_date",
    last_task_completed_date_criteria_builder=meets_home_visit_or_address_changes_triggers,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
