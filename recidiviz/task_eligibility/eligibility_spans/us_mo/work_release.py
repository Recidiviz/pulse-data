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
"""Task eligibility spans view that shows the spans of time when someone in MO is
eligible for work release.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import granted_work_release
from recidiviz.task_eligibility.criteria.general import (
    incarceration_within_24_months_of_projected_full_term_completion_date_min,
    incarceration_within_48_months_of_projected_full_term_completion_date_min,
    no_contraband_incarceration_incident_within_2_years,
    not_in_work_release,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (  # educational_score_1_while_incarcerated,
    completed_12_months_outside_clearance,
    has_first_degree_arson_or_robbery_offenses,
    institutional_risk_score_1_while_incarcerated,
    mental_health_score_3_or_below_while_incarcerated,
    no_current_or_prior_excluded_offenses_work_release,
    no_escape_in_10_years_or_current_sentence,
    not_has_first_degree_arson_or_robbery_offenses,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

NO_PROHIBITING_OFFENSES_NEAR_TERM_COMPLETION_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_MO_INCARCERATION_WITHIN_48_MONTHS_OF_PROJECTED_FULL_TERM_COMPLETION_DATE_MIN_AND_NOT_HAS_FIRST_DEGREE_ARSON_OR_ROBBERY_OFFENSES",
    sub_criteria_list=[
        not_has_first_degree_arson_or_robbery_offenses.VIEW_BUILDER,
        incarceration_within_48_months_of_projected_full_term_completion_date_min.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

PROHIBITING_OFFENSES_NEAR_TERM_COMPLETION_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_MO_INCARCERATION_WITHIN_24_MONTHS_OF_PROJECTED_FULL_TERM_COMPLETION_DATE_MIN_AND_HAS_FIRST_DEGREE_ARSON_OR_ROBBERY_OFFENSES_AND_HAS_12_MONTHS_OUTSIDE_CLEARANCE",
    sub_criteria_list=[
        has_first_degree_arson_or_robbery_offenses.VIEW_BUILDER,
        incarceration_within_24_months_of_projected_full_term_completion_date_min.VIEW_BUILDER,
        completed_12_months_outside_clearance.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[],
)

MEETS_TIME_REMAINING_REQUIREMENTS_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MO_MEETS_TIME_REMAINING_REQUIREMENTS_WORK_RELEASE",
    sub_criteria_list=[
        NO_PROHIBITING_OFFENSES_NEAR_TERM_COMPLETION_CRITERIA_GROUP,
        PROHIBITING_OFFENSES_NEAR_TERM_COMPLETION_CRITERIA_GROUP,
    ],
    allowed_duplicate_reasons_keys=[
        "offense_dates",
        "offense_statutes",
        "projected_earliest_release_date_min",
    ],
    # TODO(#45236): Revisit this aggregation
    reasons_aggregate_function_override={
        "offense_dates": "ARRAY_CONCAT_AGG",
        "offense_statutes": "ARRAY_CONCAT_AGG",
    },
)

# TODO(#42982): Un-comment lines related to
# `US_MO_EDUCATIONAL_SCORE_1_WHILE_INCARCERATED` (or just delete the criterion) once we
# decide what we're going to do with it, pending feedback from TTs.
WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA: list[
    TaskCriteriaBigQueryViewBuilder
] = [
    no_contraband_incarceration_incident_within_2_years.VIEW_BUILDER,
    # For outside clearance, we check that someone is not eligible for work release,
    # because if eligible, they should show up in the work-release opportunity instead
    # of outside clearance. We include `NOT_IN_WORK_RELEASE` as a shared criterion with
    # outside clearance here because if someone is already on work release, they won't
    # be eligible for work release but could therefore show up in the outside-clearance
    # eligibility pool, but we don't want to surface them there if they're already on
    # work release.
    not_in_work_release.VIEW_BUILDER,
    # educational_score_1_while_incarcerated.VIEW_BUILDER,
    institutional_risk_score_1_while_incarcerated.VIEW_BUILDER,
    mental_health_score_3_or_below_while_incarcerated.VIEW_BUILDER,
    no_escape_in_10_years_or_current_sentence.VIEW_BUILDER,
]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="WORK_RELEASE",
    description=__doc__,
    # TODO(#43388): Ensure that this is the correct candidate population.
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    # TODO(#42982): Finish adding in criteria and filling in stubs.
    criteria_spans_view_builders=[
        *WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
        no_current_or_prior_excluded_offenses_work_release.VIEW_BUILDER,
        MEETS_TIME_REMAINING_REQUIREMENTS_CRITERIA_GROUP,
    ],
    # TODO(#43358): Make sure this completion event is pulling in the proper data from
    # upstream to capture work-release events appropriately.
    completion_event_builder=granted_work_release.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
