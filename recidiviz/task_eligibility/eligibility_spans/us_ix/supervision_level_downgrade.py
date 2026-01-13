# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Shows the spans of time during which someone in ID may be eligible to have their supervision
level downgraded.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population_08_2020_present,
)
from recidiviz.task_eligibility.completion_events.general import (
    supervision_level_downgrade,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_not_internal_unknown,
    supervision_level_is_not_interstate_compact,
    supervision_level_is_not_residential_program,
    supervision_level_is_not_unassigned,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    served_at_least_one_year_for_dui_if_lsir_level_low,
    supervision_level_higher_than_assessment_level,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="SUPERVISION_LEVEL_DOWNGRADE",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population_08_2020_present.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_level_higher_than_assessment_level.VIEW_BUILDER,
        supervision_level_is_not_internal_unknown.VIEW_BUILDER,
        supervision_level_is_not_interstate_compact.VIEW_BUILDER,
        supervision_level_is_not_unassigned.VIEW_BUILDER,
        supervision_level_is_not_residential_program.VIEW_BUILDER,
        served_at_least_one_year_for_dui_if_lsir_level_low.VIEW_BUILDER,
    ],
    completion_event_builder=supervision_level_downgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
