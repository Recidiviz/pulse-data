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
for a furlough release.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_me import (
    granted_furlough,
)
from recidiviz.task_eligibility.criteria.state_specific.us_me import (
    custody_level_is_minimum_or_community,
    no_class_a_or_b_violation_for_90_days,
    no_detainers_warrants_or_other,
    served_30_days_at_eligible_facility_for_furlough_or_work_release,
    served_half_of_sentence,
    three_years_remaining_on_sentence,
)
from recidiviz.task_eligibility.criteria_condition import NotEligibleCriteriaCondition
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ME,
    task_name="FURLOUGH_RELEASE_FORM",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_is_minimum_or_community.VIEW_BUILDER,
        served_half_of_sentence.VIEW_BUILDER,
        three_years_remaining_on_sentence.VIEW_BUILDER,
        no_detainers_warrants_or_other.VIEW_BUILDER,
        served_30_days_at_eligible_facility_for_furlough_or_work_release.VIEW_BUILDER,
        no_class_a_or_b_violation_for_90_days.VIEW_BUILDER,
    ],
    completion_event_builder=granted_furlough.VIEW_BUILDER,
    almost_eligible_condition=NotEligibleCriteriaCondition(
        criteria=served_half_of_sentence.VIEW_BUILDER,
        description=(
            "Have not served 1/2 of sentence: folks without this condition may request a furlough, but not for family "
            "purposes"
        ),
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
