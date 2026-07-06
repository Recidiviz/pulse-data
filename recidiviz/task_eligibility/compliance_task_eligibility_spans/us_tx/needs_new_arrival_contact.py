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
"""Defines a compliance TES view that shows spans of time for which RRC supervision
clients do not meet the new-arrival field contact standard: a field (face-to-face)
contact within 2 days of arriving on an officer's caseload as a new arrival (a client
placed in an RRC with status "RELEASED - PENDING ARRIVAL").
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    prioritized_supervision_population_not_in_custody_warrant_or_unsupervised_with_officer,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    meets_new_arrival_contact_trigger,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="needs_new_arrival_contact",
    candidate_population_view_builder=prioritized_supervision_population_not_in_custody_warrant_or_unsupervised_with_officer.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_new_arrival_contact_trigger.VIEW_BUILDER,
    ],
    compliance_type=ComplianceType.CONTACT,
    cadence_type=CadenceType.NONRECURRING,
    due_date_field="contact_due_date",
    last_task_completed_date_field="last_contact_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
