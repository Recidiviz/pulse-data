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
"""Identifies spans of time during which supervision clients in MO require an in-person
contact to meet contact standards.
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_mo import (
    supervision_tasks_eligible_population,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    meets_in_person_contact_triggers,
    meets_in_person_in_community_contact_triggers,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# We use a criteria group to identify who needs an in-person contact. In order to meet
# standards for in-person contacts as a whole, a client must have enough in-person
# contacts and have met the sub-requirement that a certain number of those contacts must
# be in the community. If someone needs more in-person contacts in general or needs more
# in-person contacts in the community, we'll surface them.
IN_PERSON_CONTACT_TRIGGERS_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MO_MEETS_IN_PERSON_OR_IN_PERSON_IN_COMMUNITY_CONTACT_TRIGGERS",
    sub_criteria_list=[
        meets_in_person_contact_triggers.VIEW_BUILDER,
        meets_in_person_in_community_contact_triggers.VIEW_BUILDER,
    ],
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="needs_in_person_contact",
    # TODO(#50537): Update/refine candidate population to ensure it's correct.
    candidate_population_view_builder=supervision_tasks_eligible_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        IN_PERSON_CONTACT_TRIGGERS_CRITERIA_GROUP,
    ],
    compliance_type=ComplianceType.CONTACT,
    due_date_field="contact_due_date",
    last_task_completed_date_field="last_contact_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
