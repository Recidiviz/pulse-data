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
"""Builder for a task eligiblity spans view that shows the spans of time during which
someone in CO is eligible for geriatric parole.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import release_to_parole
from recidiviz.task_eligibility.criteria.general import (
    age_64_years_or_older,
    incarcerated_at_least_20_years,
    not_serving_for_sexual_offense,
    not_serving_for_violent_offense,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Builder for a task eligiblity spans view that shows the spans of time during which
someone in CO is eligible for geriatric parole.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_CO,
    task_name="TRANSFER_TO_GERIATRIC_PAROLE",
    description=_DESCRIPTION,
    candidate_population_view_builder=incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        age_64_years_or_older.VIEW_BUILDER,
        incarcerated_at_least_20_years.VIEW_BUILDER,
        not_serving_for_sexual_offense.VIEW_BUILDER,
        not_serving_for_violent_offense.VIEW_BUILDER,
    ],
    completion_event_builder=release_to_parole.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
