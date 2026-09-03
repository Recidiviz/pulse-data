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

"""ORAS Community Supervision Tool (CST) required for active MO parole population.
As defined by P3-2.3"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_mo import (
    assessment_tasks_eligible_population,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    is_missing_annual_cst_reassessment,
    scored_low_risk_on_last_cst,
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

_CRITERIA_NAME = "US_MO_MEETS_CST_REASSESSMENT_ELIGIBILITY_CRITERIA"

meets_cst_reassessment_eligibility_criteria = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    logic_type=TaskCriteriaGroupLogicType.AND,
    sub_criteria_list=[
        is_missing_annual_cst_reassessment.VIEW_BUILDER,
        StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=scored_low_risk_on_last_cst.VIEW_BUILDER
        ),
        # TODO(MO-2): Reinstate a 120-day-before-max-expiration suppression
        # criterion once probation end-date ingest reliability for
        # dual-supervision clients has been investigated.
    ],
)

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="needs_cst_reassessment",
    candidate_population_view_builder=assessment_tasks_eligible_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_cst_reassessment_eligibility_criteria,
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    cadence_type=CadenceType.RECURRING_ROLLING,
    due_date_field="assessment_due_date",
    display_due_date_field="assessment_display_due_date",
    last_task_completed_date_field="most_recent_assessment_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
