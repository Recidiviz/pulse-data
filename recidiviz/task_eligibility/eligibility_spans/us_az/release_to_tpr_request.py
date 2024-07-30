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
"""Shows the eligibility spans for residents in AZ who are eligible for a TPR release.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum,
    not_serving_for_sexual_offense,
    not_serving_for_violent_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    serving_assault_or_aggravated_assault_or_robbery,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the eligibility spans for residents in AZ
 who are eligible for a TPR release.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="RELEASE_TO_TPR_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum.VIEW_BUILDER,
        not_serving_for_sexual_offense.VIEW_BUILDER,
        # TODO(#31768): we have to fix this bug
        OrTaskCriteriaGroup(
            criteria_name="US_AZ_SERVING_NONVIOLENT_OFFENSE_WITH_B1B_EXCEPTIONS",
            sub_criteria_list=[
                not_serving_for_violent_offense.VIEW_BUILDER,
                serving_assault_or_aggravated_assault_or_robbery.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
