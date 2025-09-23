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
"""Builder for a task eligibility spans view that shows the spans of time during which
someone in TN is eligible for full term discharge from supervision or is 1 day away from being eligible.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    probation_parole_dual_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import full_term_discharge
from recidiviz.task_eligibility.criteria.general import (
    has_active_sentence,
    supervision_level_is_not_internal_unknown,
    supervision_level_is_not_interstate_compact,
    supervision_past_full_term_completion_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_zero_tolerance_codes_spans,
    not_on_life_sentence_or_lifetime_supervision,
    supervision_level_raw_text_is_not_wrb,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION",
    description=__doc__,
    candidate_population_view_builder=probation_parole_dual_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_past_full_term_completion_date.VIEW_BUILDER,
        not_on_life_sentence_or_lifetime_supervision.VIEW_BUILDER,
        no_zero_tolerance_codes_spans.VIEW_BUILDER,
        has_active_sentence.VIEW_BUILDER,
        supervision_level_is_not_internal_unknown.VIEW_BUILDER,
        supervision_level_is_not_interstate_compact.VIEW_BUILDER,
        supervision_level_raw_text_is_not_wrb.VIEW_BUILDER,
    ],
    completion_event_builder=full_term_discharge.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=supervision_past_full_term_completion_date.VIEW_BUILDER,
        reasons_date_field="eligible_date",
        interval_length=1,
        interval_date_part=BigQueryDateInterval.DAY,
        description="Within 1 day of full term discharge date",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
