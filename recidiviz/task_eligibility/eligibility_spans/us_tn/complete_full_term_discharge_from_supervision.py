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
someone in TN is eligible for full term discharge from supervision or is 60 days away from being eligible.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    supervision_population_all_eligible_levels,
)
from recidiviz.task_eligibility.completion_events import full_term_discharge
from recidiviz.task_eligibility.criteria.general import (
    supervision_past_full_term_completion_date_or_upcoming_60_day,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_zero_tolerance_codes_spans,
    not_on_life_sentence_or_lifetime_supervision,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time during which someone in TN is eligible
for full term discharge from supervision or is 60 days away from being eligible.
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION",
    description=_DESCRIPTION,
    candidate_population_view_builder=supervision_population_all_eligible_levels.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_past_full_term_completion_date_or_upcoming_60_day.VIEW_BUILDER,
        not_on_life_sentence_or_lifetime_supervision.VIEW_BUILDER,
        no_zero_tolerance_codes_spans.VIEW_BUILDER,
    ],
    completion_event_builder=full_term_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
