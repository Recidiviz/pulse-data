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
Shows the spans of time during which someone in ME is eligible
for a classification level downgrade to Medium Trustee status
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    custody_level_downgrade,
)
from recidiviz.task_eligibility.criteria.state_specific.us_me import (
    custody_level_is_medium,
    no_violation_for_5_years,
    five_years_remaining_on_sentence,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in ME is eligible
for a classification level downgrade to Medium Trustee status
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ME,
    task_name="CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # TODO(#25311): Add 'not already a Medium trustee' criteria once information from state is received
        custody_level_is_medium.VIEW_BUILDER,
        no_violation_for_5_years.VIEW_BUILDER,
        five_years_remaining_on_sentence.VIEW_BUILDER,
    ],
    completion_event_builder=custody_level_downgrade.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
