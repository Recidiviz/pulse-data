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
"""Shows the spans of time during which someone in AR is eligible
for the [Act] "309" program for institutional worker status in city and county jails.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    incarceration_population_state_prison,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_ar import (
    granted_institutional_worker_status,
)
from recidiviz.task_eligibility.criteria.general import (
    age_25_years_or_older,
    incarcerated_at_least_6_months,
    incarceration_within_10_years_of_parole_eligibility_date,
    no_incarceration_sanctions_within_90_days,
    not_serving_a_life_sentence,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar import (
    class_i,
    eligible_criminal_history_309,
    no_filed_but_undisposed_detainers,
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
    state_code=StateCode.US_AR,
    task_name="institutional_worker_status",
    description=__doc__,
    candidate_population_view_builder=incarceration_population_state_prison.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # TODO(#34322): Clarify Class I eligibility criteria
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.OR,
            criteria_name="US_AR_CLASS_I_OR_ELIGIBLE_FOR_CLASS_I_309",
            # Someone is eligible for the 309 program if they are in the Class I good time earning
            # class, *or* they are eligible for Class I -- the only criterion that applies to
            # Class I eligibility that doesn't also apply independently to 309 eligibility is that
            # someone must be at least 25 years old. This criteria group exists in case someone
            # is in Class I but is not "eligible" for Class I (i.e., they are younger than 25).
            # See Page 2 here: https://file.notion.so/f/f/eef9fb8f-9f1f-4309-98ff-e6cff9a43cf9/02adc886-33b0-435a-a581-f6dc76668877/AD_2018-01_Class_Status_and_Promotion_Eligibility.pdf?table=block&id=f789c2a5-c56f-482d-8e7e-a8a0921340bc&spaceId=eef9fb8f-9f1f-4309-98ff-e6cff9a43cf9&expirationTimestamp=1728172800000&signature=VJwMMbt5YH23SI_ChU2No-7UgRjEb1y0Cq5R49hmjMQ&downloadName=AD+2018-01+Class+Status+and+Promotion+Eligibility.pdf
            # for more information on Class I eligibility
            sub_criteria_list=[
                age_25_years_or_older.VIEW_BUILDER,
                class_i.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[],
        ),
        eligible_criminal_history_309.VIEW_BUILDER,
        incarcerated_at_least_6_months.VIEW_BUILDER,
        no_incarceration_sanctions_within_90_days.VIEW_BUILDER,
        not_serving_a_life_sentence.VIEW_BUILDER,
        incarceration_within_10_years_of_parole_eligibility_date.VIEW_BUILDER,
        no_filed_but_undisposed_detainers.VIEW_BUILDER,
    ],
    completion_event_builder=granted_institutional_worker_status.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
