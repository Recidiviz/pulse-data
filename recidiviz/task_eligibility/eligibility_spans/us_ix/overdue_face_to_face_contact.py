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

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_ix import (
    active_supervision_population_for_tasks,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ix import (
    face_to_face_contact,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    meets_overdue_face_to_face_contact_alert,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="OVERDUE_FACE_TO_FACE_CONTACT",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population_for_tasks.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_overdue_face_to_face_contact_alert.VIEW_BUILDER,
    ],
    completion_event_builder=face_to_face_contact.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
