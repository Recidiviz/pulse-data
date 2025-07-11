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
"""Task eligibility spans view that shows the spans of time when someone in MO is
eligible for outside clearance.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mo import (
    granted_institutional_worker_status,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    mental_health_score_3_or_below_while_incarcerated,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="OUTSIDE_CLEARANCE",
    description=__doc__,
    # TODO(#44398): Ensure that this is the correct candidate population.
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    # TODO(#44404): Finish adding in criteria and filling in stubs.
    criteria_spans_view_builders=[
        mental_health_score_3_or_below_while_incarcerated.VIEW_BUILDER,
    ],
    # TODO(#44389): Implement the correct completion event (either general or state-
    # specific) for this opportunity.
    completion_event_builder=granted_institutional_worker_status.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
