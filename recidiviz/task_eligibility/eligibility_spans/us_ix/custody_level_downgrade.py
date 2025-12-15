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
someone in Idaho is eligible for a custody level downgrade.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population_state_prison,
)
from recidiviz.task_eligibility.completion_events.general import custody_level_downgrade
from recidiviz.task_eligibility.criteria.general import (
    custody_level_higher_than_recommended,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    no_active_discretionary_override,
    not_already_on_lowest_eligible_custody_level,
    not_in_classification_pilot,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    task_name="CUSTODY_LEVEL_DOWNGRADE",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population_state_prison.VIEW_BUILDER,
    criteria_spans_view_builders=[
        custody_level_higher_than_recommended.VIEW_BUILDER,
        no_active_discretionary_override.VIEW_BUILDER,
        not_in_classification_pilot.VIEW_BUILDER,
        not_already_on_lowest_eligible_custody_level.VIEW_BUILDER,
    ],
    completion_event_builder=custody_level_downgrade.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=custody_level_higher_than_recommended.VIEW_BUILDER,
        reasons_date_field="upcoming_eligibility_date",
        interval_length=1,
        interval_date_part=BigQueryDateInterval.MONTH,
        description="Within 1 month of eligibility",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
