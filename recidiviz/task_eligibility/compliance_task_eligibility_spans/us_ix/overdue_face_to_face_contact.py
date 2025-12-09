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
Defines a criteria view that shows spans of time when supervision clients are overdue 
for a face-to-face contact. We will consider someone as overdue when the following
is true for each supervision level:

- High: > 1 month without a contact
- Moderate: > 2 month without a contact
- Low: > 7 month without a contact
- SO High: > 1 month without a contact
- SO Moderate: > 2 month without a contact
- SO Low: > 4 month without a contact
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
    meets_overdue_face_to_face_contact_alert,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="overdue_face_to_face_contact",
    candidate_population_view_builder=active_supervision_population_for_tasks.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_overdue_face_to_face_contact_alert.VIEW_BUILDER,
    ],
    compliance_type=ComplianceType.CONTACT,
    due_date_field="overdue_for_contact_alert_date",
    last_task_completed_date_field="last_contact_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
