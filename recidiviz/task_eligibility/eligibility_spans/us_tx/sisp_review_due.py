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
"""Shows the spans of time during which someone in TX, who is on the Special
Intensive Supervision Program (SISP), has a SISP review currently due —
initial, recurring High (annual), or recurring non-High (semi-annual).
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tx import (
    supervision_level_review,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    sisp_initial_review_due_non_high,
    sisp_recurring_review_due_high,
    sisp_recurring_review_due_non_high,
    supervision_case_type_is_intense_supervision,
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

ANY_SISP_REVIEW_DUE = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name="US_TX_ANY_SISP_REVIEW_DUE",
    sub_criteria_list=[
        sisp_initial_review_due_non_high.VIEW_BUILDER,
        sisp_recurring_review_due_high.VIEW_BUILDER,
        sisp_recurring_review_due_non_high.VIEW_BUILDER,
    ],
    # Shared across all three sub-criteria.
    allowed_duplicate_reasons_keys=[
        "contact_cadence",
        "contact_count",
        "contact_due_date",
        "overdue_flag",
        # Shared across the two recurring sub-criteria.
        "contact_period_end",
        "contact_period_start",
        "contacts_needed",
        "earliest_unmet_due_date",
        "frequency",
        "frequency_date_part",
        "last_contact_date",
        "period_type",
        "scheduled_contact_dates",
        "type_of_contact",
    ],
    # Initial + recurring non-High can both fire during the initial review's
    # leading window; pick the earliest upcoming deadline.
    reasons_aggregate_function_override={"contact_due_date": "MIN"},
)

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="SISP_REVIEW_DUE",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_case_type_is_intense_supervision.VIEW_BUILDER,
        ANY_SISP_REVIEW_DUE,
    ],
    completion_event_builder=supervision_level_review.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
