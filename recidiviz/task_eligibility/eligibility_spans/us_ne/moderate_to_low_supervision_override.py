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
"""Shows the spans of time during which someone in NE is eligible
for an override From Moderate to Low supervision.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ne import (
    override_to_low_supervision,
)
from recidiviz.task_eligibility.criteria.general import supervision_level_is_medium
from recidiviz.task_eligibility.eligibility_spans.us_ne.conditional_low_risk_supervision_override import (
    US_NE_GENERAL_SUPERVISION_LEVEL_OVERRIDE_CRITERIA,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="MODERATE_TO_LOW_SUPERVISION_OVERRIDE",
    description=__doc__,
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        criterion.VIEW_BUILDER
        for criterion in US_NE_GENERAL_SUPERVISION_LEVEL_OVERRIDE_CRITERIA
    ]
    + [
        supervision_level_is_medium.VIEW_BUILDER,
    ],
    completion_event_builder=override_to_low_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
