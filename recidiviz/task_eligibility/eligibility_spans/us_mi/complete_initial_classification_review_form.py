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
"""Shows the spans of time during which someone in MI is eligible for their initial classification review"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_mi import (
    supervision_level_downgrade_after_initial_classification_review_date,
)
from recidiviz.task_eligibility.criteria.general import (
    supervision_level_is_not_limited,
    supervision_not_past_full_term_completion_date_or_upcoming_90_days,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    not_already_on_lowest_eligible_supervision_level,
    not_on_electronic_monitoring,
    not_on_lifetime_electronic_monitoring,
    past_initial_classification_review_date,
    supervision_level_is_not_minimum_low,
    supervision_level_is_not_modified,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_INITIAL_CLASSIFICATION_REVIEW_FORM",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        past_initial_classification_review_date.VIEW_BUILDER,
        not_already_on_lowest_eligible_supervision_level.VIEW_BUILDER,
        not_on_lifetime_electronic_monitoring.VIEW_BUILDER,
        not_on_electronic_monitoring.VIEW_BUILDER,
        supervision_level_is_not_limited.VIEW_BUILDER,
        supervision_level_is_not_minimum_low.VIEW_BUILDER,
        supervision_level_is_not_modified.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date_or_upcoming_90_days.VIEW_BUILDER,
    ],
    completion_event_builder=supervision_level_downgrade_after_initial_classification_review_date.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=past_initial_classification_review_date.VIEW_BUILDER,
        reasons_date_field="classification_review_date",
        interval_length=30,
        interval_date_part=BigQueryDateInterval.DAY,
        description="Within 30 days of initial classification review date",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
