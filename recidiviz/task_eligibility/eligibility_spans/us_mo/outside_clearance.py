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
eligible for outside clearance.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    granted_institutional_worker_status,
)
from recidiviz.task_eligibility.criteria.general import (
    not_on_institutional_worker_status,
    within_60_months_of_projected_full_term_release_date_min,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mo import (
    within_60_months_of_earliest_established_release_date_itim,
)
from recidiviz.task_eligibility.eligibility_spans.us_mo.work_release import (
    WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# If someone is within 60 months of their earliest release date (either according to
# ingested data or to the data we'd see in the ITIM screen), then we'll consider them to
# be within 60 months of release.
# TODO(#46222): Revisit this logic and see if we want to adjust how we're handling the
# dates in MO.
WITHIN_60_MONTHS_CRITERIA_GROUP = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_MO_WITHIN_60_MONTHS_OF_EARLIEST_RELEASE_DATE",
    sub_criteria_list=[
        within_60_months_of_projected_full_term_release_date_min.VIEW_BUILDER,
        within_60_months_of_earliest_established_release_date_itim.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "earliest_release_date",
    ],
    reasons_aggregate_function_override={
        "earliest_release_date": "MIN",
    },
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    task_name="OUTSIDE_CLEARANCE",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        *WORK_RELEASE_AND_OUTSIDE_CLEARANCE_SHARED_CRITERIA,
        WITHIN_60_MONTHS_CRITERIA_GROUP,
        not_on_institutional_worker_status.VIEW_BUILDER,
    ],
    completion_event_builder=granted_institutional_worker_status.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
