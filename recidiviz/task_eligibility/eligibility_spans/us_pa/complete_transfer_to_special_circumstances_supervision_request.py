# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Shows the spans of time during which someone in PA is eligible for transfer
to Special Circumstances supervision."""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_pa import (
    transfer_to_special_circumstances_supervision,
)
from recidiviz.task_eligibility.criteria.general import supervision_level_is_not_limited
from recidiviz.task_eligibility.criteria.state_specific.us_pa import (
    fulfilled_requirements,
    meets_one_of_special_circumstances_criteria,
    no_high_sanctions_in_past_year,
    not_eligible_for_admin_supervision,
    supervision_level_is_not_special_circumstances,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which
someone in PA is eligible for transfer to Special Circumstances supervision.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_PA,
    task_name="COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_is_not_limited.VIEW_BUILDER,
        supervision_level_is_not_special_circumstances.VIEW_BUILDER,
        no_high_sanctions_in_past_year.VIEW_BUILDER,
        fulfilled_requirements.VIEW_BUILDER,
        not_eligible_for_admin_supervision.VIEW_BUILDER,
        meets_one_of_special_circumstances_criteria.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_special_circumstances_supervision.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
