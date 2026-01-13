# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
Shows the spans of time during which someone in CA is eligible
for a SLD (Supervised Level Downgrade)
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    supervision_level_downgrade,
)
from recidiviz.task_eligibility.criteria.general import (
    no_supervision_violation_within_6_months,
    supervision_level_is_high_for_6_months,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ca import (
    assessment_level_3_or_lower,
    housing_type_is_not_transient,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_CA,
    task_name="supervision_level_downgrade",
    description=__doc__,
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        assessment_level_3_or_lower.VIEW_BUILDER,
        supervision_level_is_high_for_6_months.VIEW_BUILDER,
        housing_type_is_not_transient.VIEW_BUILDER,
        no_supervision_violation_within_6_months.VIEW_BUILDER,
    ],
    completion_event_builder=supervision_level_downgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
