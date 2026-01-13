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
for a reclassification
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_me import (
    incarceration_assessment_completed,
)
from recidiviz.task_eligibility.criteria.state_specific.us_me import (
    incarceration_past_annual_classification_date,
    incarceration_past_semi_annual_classification_date,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_PAST_RELEVANT_CLASSIFICATION_DATE_VIEW_BUILDER = (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
        logic_type=TaskCriteriaGroupLogicType.OR,
        criteria_name="US_ME_INCARCERATION_PAST_RELEVANT_CLASSIFICATION_DATE",
        sub_criteria_list=[
            incarceration_past_annual_classification_date.VIEW_BUILDER,
            incarceration_past_semi_annual_classification_date.VIEW_BUILDER,
        ],
        allowed_duplicate_reasons_keys=[
            "reclass_type",
            "reclasses_needed",
            "latest_classification_date",
            "eligible_date",
        ],
    )
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ME,
    task_name="CUSTODY_RECLASSIFICATION_REVIEW_FORM",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        INCARCERATION_PAST_RELEVANT_CLASSIFICATION_DATE_VIEW_BUILDER,
    ],
    completion_event_builder=incarceration_assessment_completed.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=INCARCERATION_PAST_RELEVANT_CLASSIFICATION_DATE_VIEW_BUILDER,
        reasons_date_field="eligible_date",
        interval_length=1,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 1 month of reclassification reset date",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
