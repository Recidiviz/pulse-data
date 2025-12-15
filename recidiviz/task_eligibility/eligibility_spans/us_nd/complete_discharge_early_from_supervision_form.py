# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Shows the spans of time during which someone in ND is eligible
to have early termination paperwork completed for them in preparation for an early
discharge.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    supervision_early_discharge_before_full_term_completion_date,
    supervision_not_past_full_term_completion_date,
    supervision_past_early_discharge_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd import (
    implied_valid_early_termination_sentence_type,
    implied_valid_early_termination_supervision_level,
    not_in_active_revocation_status,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    task_name="COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_FORM",
    description=__doc__,
    candidate_population_view_builder=active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        implied_valid_early_termination_sentence_type.VIEW_BUILDER,
        implied_valid_early_termination_supervision_level.VIEW_BUILDER,
        not_in_active_revocation_status.VIEW_BUILDER,
        supervision_early_discharge_before_full_term_completion_date.VIEW_BUILDER,
        supervision_past_early_discharge_date.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=supervision_past_early_discharge_date.VIEW_BUILDER,
        reasons_date_field="early_discharge_due_date",
        interval_length=3,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 3 months of supervision early discharge date",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
