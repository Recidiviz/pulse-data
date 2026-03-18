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
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Defines a criteria view that shows spans of time for which supervision clients
need a home contact task completed. This happens in the following cases:
- The client needs an initial home contact (within 30 days of starting supervision)
- The client needs a  subsequent contact (every 30 or 60 days after the last home contact)
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
from recidiviz.task_eligibility.criteria.state_specific.us_co import (
    meets_initial_home_contact_triggers,
    meets_subsequent_home_contact_triggers,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

home_contact_trigger_view_builder = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_CO_MEETS_HOME_CONTACT_TRIGGER",
    sub_criteria_list=[
        meets_initial_home_contact_triggers.VIEW_BUILDER,
        meets_subsequent_home_contact_triggers.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "contact_due_date",
        "last_contact_date",
        "contact_cadence",
    ],
    reasons_aggregate_function_override={
        "contact_due_date": "MIN",
        "last_contact_date": "MAX",
    },
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_CO,
    task_name="needs_home_contact",
    candidate_population_view_builder=parole_active_supervision_including_null_supervision_level_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        home_contact_trigger_view_builder,
    ],
    compliance_type=ComplianceType.CONTACT,
    cadence_type=CadenceType.RECURRING_ROLLING,
    due_date_field="contact_due_date",
    due_date_criteria_builder=home_contact_trigger_view_builder,
    last_task_completed_date_field="last_contact_date",
    last_task_completed_date_criteria_builder=home_contact_trigger_view_builder,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
