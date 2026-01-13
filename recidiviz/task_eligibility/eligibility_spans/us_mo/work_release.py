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
    not_in_work_release,
    within_24_months_of_projected_full_term_release_date_min,
    within_48_months_of_projected_full_term_release_date_min,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    educational_score_1,
    has_first_degree_arson_or_robbery_offenses,
    institutional_risk_score_1_while_incarcerated,
    mental_health_score_3_or_below_while_incarcerated,
    no_11_2_incarceration_incident_within_2_years,
    no_current_or_prior_excluded_offenses_work_release,
    no_escape_in_10_years_or_current_cycle,
    not_has_first_degree_arson_or_robbery_offenses,
    not_on_work_release_assignment,
    within_24_months_of_earliest_established_release_date_itim,
    within_48_months_of_earliest_established_release_date_itim,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
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

# If someone is within 48 months of their earliest release date (either according to
# ingested data or to the data we'd see in the ITIM screen), then we'll consider them to
# be within 48 months of release.
# TODO(#46222): Revisit this logic and see if we want to adjust how we're handling the
# dates in MO.
WITHIN_48_MONTHS_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MO_WITHIN_48_MONTHS_OF_EARLIEST_RELEASE_DATE",
    sub_criteria_list=[
        within_48_months_of_projected_full_term_release_date_min.VIEW_BUILDER,
        within_48_months_of_earliest_established_release_date_itim.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "earliest_release_date",
    ],
    reasons_aggregate_function_override={
        "earliest_release_date": "MIN",
    },
)

# If someone is within 24 months of their earliest release date (either according to
# ingested data or to the data we'd see in the ITIM screen), then we'll consider them to
# be within 24 months of release.
# TODO(#46222): Revisit this logic and see if we want to adjust how we're handling the
# dates in MO.
WITHIN_24_MONTHS_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MO_WITHIN_24_MONTHS_OF_EARLIEST_RELEASE_DATE",
    sub_criteria_list=[
        within_24_months_of_projected_full_term_release_date_min.VIEW_BUILDER,
        within_24_months_of_earliest_established_release_date_itim.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "earliest_release_date",
    ],
    reasons_aggregate_function_override={
        "earliest_release_date": "MIN",
    },
)

NO_PROHIBITING_OFFENSES_NEAR_TERM_COMPLETION_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_MO_WITHIN_48_MONTHS_OF_EARLIEST_RELEASE_DATE_AND_NOT_HAS_FIRST_DEGREE_ARSON_OR_ROBBERY_OFFENSES",
    sub_criteria_list=[
        not_has_first_degree_arson_or_robbery_offenses.VIEW_BUILDER,
        WITHIN_48_MONTHS_CRITERIA_GROUP,
    ],
    allowed_duplicate_reasons_keys=[],
)

PROHIBITING_OFFENSES_NEAR_TERM_COMPLETION_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.AND,
    criteria_name="US_MO_WITHIN_24_MONTHS_OF_EARLIEST_RELEASE_DATE_AND_HAS_FIRST_DEGREE_ARSON_OR_ROBBERY_OFFENSES",
    sub_criteria_list=[
        # NB: The below criterion identifies people who have first-degree arson or
        # robbery offenses we can positively identify, but it is not expected to catch
        # all such offenses. In the tool, we will still instruct users to check a
        # resident's offense history for these offenses when screening for work release.
        has_first_degree_arson_or_robbery_offenses.VIEW_BUILDER,
        WITHIN_24_MONTHS_CRITERIA_GROUP,
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
        "disqualifying_offenses",
        "disqualifying_dates",
        "earliest_release_date",
        "earliest_release_date_types",
    ],
    reasons_aggregate_function_override={
        # The reasons blobs should be identical in each of these cases (since the two
        # underlying sub-criteria are just inversions of each other), so we can use
        # ANY_VALUE here to just pick one.
        "disqualifying_offenses": "ANY_VALUE",
        "disqualifying_dates": "ANY_VALUE",
        # Should be identical across criteria, since the same underlying logic is used
        # for each of the release-date criteria, so it shouldn't matter which one we
        # pick.
        "earliest_release_date_types": "ANY_VALUE",
    },
)

# If someone is approved for work release already (which is captured by
# `NOT_IN_WORK_RELEASE`) or has an active work-release assignment (which is captured by
# `US_MO_NOT_ON_WORK_RELEASE_ASSIGNMENT`), they will not meet this group criterion. We
# use this group to ensure that we only surface people who are not already authorized
# for work release. Since not every facility records approvals/denials via the
# work-release/outside-clearance requests table, we also check for active work-release
# assignments in the `US_MO_NOT_ON_WORK_RELEASE_ASSIGNMENT` criterion as another way to
# identify residents who are already on work release.
NOT_ALREADY_ON_WORK_RELEASE_CRITERIA_GROUP = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.AND,
        criteria_name="US_MO_NOT_IN_WORK_RELEASE_AND_NOT_ON_WORK_RELEASE_ASSIGNMENT",
        sub_criteria_list=[
            not_in_work_release.VIEW_BUILDER,
            not_on_work_release_assignment.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[],
    )
)

WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA: list[
    TaskCriteriaBigQueryViewBuilder
] = [
    institutional_risk_score_1_while_incarcerated.VIEW_BUILDER,
    mental_health_score_3_or_below_while_incarcerated.VIEW_BUILDER,
    # NB: The below criterion identifies people who have 11.2 incidents, which are
    # definitively disqualifying for 2 years. It's possible that other incidents have
    # been disqualifying for any given resident as well, but other incident types are
    # considered on a case-by-case basis, while this one is always disqualifying. In the
    # tool, we will still instruct users to check a resident's incident history when
    # screening for work release and/or outside clearance.
    no_11_2_incarceration_incident_within_2_years.VIEW_BUILDER,
    no_escape_in_10_years_or_current_cycle.VIEW_BUILDER,
    # For outside clearance, we check that someone is not eligible for work release,
    # because if eligible, they should show up in the work-release opportunity instead
    # of outside clearance. We include `NOT_IN_WORK_RELEASE` as a shared criterion with
    # outside clearance here because if someone is already on work release, they won't
    # be eligible for work release but could therefore show up in the outside-clearance
    # eligibility pool, but we don't want to surface them there if they're already on
    # work release.
    NOT_ALREADY_ON_WORK_RELEASE_CRITERIA_GROUP,
]

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="WORK_RELEASE",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        *WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
        educational_score_1.VIEW_BUILDER,
        # NB: The below criterion identifies people who have excluded offenses we can
        # positively identify, but it is not expected to catch all excluded offenses. In
        # the tool, we will still instruct users to check a resident's offense history
        # for excluded offenses when screening for work release.
        no_current_or_prior_excluded_offenses_work_release.VIEW_BUILDER,
        # TODO(#46222): Do we need to update the criteria going into this group to
        # consider the right set of release dates in MO? What date(s) are we using right
        # now for the sub-criteria in this group?
        MEETS_TIME_REMAINING_REQUIREMENTS_CRITERIA_GROUP,
    ],
    completion_event_builder=granted_work_release.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
