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
"""Selects all spans of time in which a person is a candidate for DTP in Arizona. This includes people who meet the
following criteria:
- In Incarceration Candidate Population
- No ineligible DTP offense convictions
- Only drug offense convictions
- No DTP Denial or Release in Current Incarceration
- No DTP Removals from Self Improvement Programs
- Not serving flat sentence
- Is US Citizen or Legal Permanent Resident
- No major violent violation during incarceration
"""

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az import (
    acis_dtp_date_not_set,
    is_us_citizen_or_legal_permanent_resident,
    no_dtp_denial_or_previous_dtp_release,
    no_dtp_removals_from_self_improvement_programs,
    no_ineligible_dtp_offense_convictions,
    no_major_violent_violation_during_incarceration,
    not_serving_flat_sentence,
    only_drug_offense_convictions,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_AZ_DTP_ELIGIBLE_POPULATION"

_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name=_POPULATION_NAME,
    sub_criteria_list=[
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_AZ_NOT_PERMANENTLY_INELIGIBLE_FOR_DTP",
            sub_criteria_list=[
                no_ineligible_dtp_offense_convictions.VIEW_BUILDER,
                only_drug_offense_convictions.VIEW_BUILDER,
                no_dtp_denial_or_previous_dtp_release.VIEW_BUILDER,
                no_dtp_removals_from_self_improvement_programs.VIEW_BUILDER,
                not_serving_flat_sentence.VIEW_BUILDER,
                is_us_citizen_or_legal_permanent_resident.VIEW_BUILDER,
                no_major_violent_violation_during_incarceration.VIEW_BUILDER,
                incarceration_population.VIEW_BUILDER.as_criteria(
                    criteria_name="IN_INCARCERATION_CANDIDATE_POPULATION",
                    sessions_dataset=SESSIONS_DATASET,
                ),
            ],
            allowed_duplicate_reasons_keys=["ineligible_offenses"],
            reasons_aggregate_function_override={
                "ineligible_offenses": "ARRAY_CONCAT_AGG"
            },
        ),
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_AZ_HAS_DTP_DATE",
            sub_criteria_list=[
                StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
                    sub_criteria=acis_dtp_date_not_set.VIEW_BUILDER,
                ),
                incarceration_population.VIEW_BUILDER.as_criteria(
                    criteria_name="IN_INCARCERATION_CANDIDATE_POPULATION",
                    sessions_dataset=SESSIONS_DATASET,
                ),
            ],
        ),
    ],
    allowed_duplicate_reasons_keys=["population_name"],
)

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder.from_criteria_group(
        criteria_group=_CRITERIA_GROUP, population_name=_POPULATION_NAME
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
