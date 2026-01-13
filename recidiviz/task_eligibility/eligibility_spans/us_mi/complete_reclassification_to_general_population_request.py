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
"""Shows the spans of time during which someone in MI is eligible to be reclassified to general
 population from solitary confinement."""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    general_incarceration_population,
)
from recidiviz.task_eligibility.completion_events.general import (
    transfer_out_of_disciplinary_or_temporary_solitary_confinement,
)
from recidiviz.task_eligibility.criteria.general import (
    housing_unit_type_is_solitary_confinement,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    eligible_for_reclassification_from_solitary_to_general,
)
from recidiviz.task_eligibility.criteria_condition import TimeDependentCriteriaCondition
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=general_incarceration_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        eligible_for_reclassification_from_solitary_to_general.VIEW_BUILDER,
        housing_unit_type_is_solitary_confinement.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_out_of_disciplinary_or_temporary_solitary_confinement.VIEW_BUILDER,
    almost_eligible_condition=TimeDependentCriteriaCondition(
        criteria=eligible_for_reclassification_from_solitary_to_general.VIEW_BUILDER,
        reasons_date_field="overdue_in_temporary_date",
        interval_length=23,
        interval_date_part=BigQueryDateInterval.DAY,
        description="Spent at least 7 days in temporary segregation",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
