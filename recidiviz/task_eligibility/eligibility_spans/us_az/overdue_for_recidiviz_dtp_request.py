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
"""Shows the eligibility spans for residents in AZ who are eligible for a Drug Transition 
Program (DTP) release according to our (Recidiviz) calculations. 
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    custody_level_is_minimum_or_medium,
    no_nonviolent_incarceration_violation_within_6_months,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    enrolled_in_functional_literacy,
    is_us_citizen_or_legal_permanent_resident,
    meets_functional_literacy,
    no_active_felony_detainers,
    no_arson_conviction,
    no_dangerous_crimes_against_children_conviction,
    no_domestic_violence_conviction,
    no_drug_offense_conviction,
    no_dtp_denial_in_current_incarceration,
    no_dtp_removals_from_self_improvement_programs,
    no_major_violent_violation_during_incarceration,
    no_sexual_exploitation_of_children_conviction,
    no_sexual_offense_conviction,
    no_unsatisfactory_program_ratings_within_3_months,
    no_violent_conviction,
    not_previous_dtp_participant,
    time_90_days_before_release,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    OrTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the eligibility spans for residents in AZ who are eligible for a Drug Transition
Program (DTP) release according to our (Recidiviz) calculations. 
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_AZ,
    task_name="OVERDUE_FOR_RECIDIVIZ_DTP_REQUEST",
    description=_DESCRIPTION,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        time_90_days_before_release.VIEW_BUILDER,
        no_sexual_offense_conviction.VIEW_BUILDER,
        no_drug_offense_conviction.VIEW_BUILDER,
        no_arson_conviction.VIEW_BUILDER,
        no_domestic_violence_conviction.VIEW_BUILDER,
        no_sexual_exploitation_of_children_conviction.VIEW_BUILDER,
        no_violent_conviction.VIEW_BUILDER,
        no_dangerous_crimes_against_children_conviction.VIEW_BUILDER,
        no_active_felony_detainers.VIEW_BUILDER,
        custody_level_is_minimum_or_medium.VIEW_BUILDER,
        no_nonviolent_incarceration_violation_within_6_months.VIEW_BUILDER,
        no_major_violent_violation_during_incarceration.VIEW_BUILDER,
        is_us_citizen_or_legal_permanent_resident.VIEW_BUILDER,
        no_unsatisfactory_program_ratings_within_3_months.VIEW_BUILDER,
        no_dtp_removals_from_self_improvement_programs.VIEW_BUILDER,
        OrTaskCriteriaGroup(
            criteria_name="US_AZ_ENROLLED_IN_OR_MEETS_MANDATORY_LITERACY",
            sub_criteria_list=[
                meets_functional_literacy.VIEW_BUILDER,
                enrolled_in_functional_literacy.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        not_previous_dtp_participant.VIEW_BUILDER,
        no_dtp_denial_in_current_incarceration.VIEW_BUILDER,
    ],
    # TODO(#33655): Update this to the correct task completion event
    completion_event_builder=early_discharge.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
