# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shows the spans of time during which someone in MI is in ad seg and eligible for
a security classification committee review"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.state_specific.us_mi import (
    ad_seg_scc_solitary_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mi import (
    security_classification_committee_review,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    eligible_for_initial_ad_seg_security_classification_committee_review,
    thirty_days_past_last_ad_seg_security_classification_committee_review_date,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_AD_SEG_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
    description=__doc__,
    candidate_population_view_builder=ad_seg_scc_solitary_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_MI_PAST_AD_SEG_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_DATE",
            sub_criteria_list=[
                eligible_for_initial_ad_seg_security_classification_committee_review.VIEW_BUILDER,
                thirty_days_past_last_ad_seg_security_classification_committee_review_date.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=["eligibility_type", "next_scc_due_date"],
        ),
    ],
    completion_event_builder=security_classification_committee_review.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
