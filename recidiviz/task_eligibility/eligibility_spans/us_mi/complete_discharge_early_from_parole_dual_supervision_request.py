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
"""Shows the spans of time during which someone in MI is eligible
to request early discharge from supervision through the parole board.
"""
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_dual_active_supervision_and_supervision_out_of_state_population,
)
from recidiviz.task_eligibility.completion_events.general import early_discharge
from recidiviz.task_eligibility.criteria.general import (
    custodial_authority_is_supervision_authority_or_other_state,
    serving_at_least_one_year_on_parole_supervision_or_supervision_out_of_state,
    supervision_not_past_full_term_completion_date_or_upcoming_30_days,
    supervision_or_supervision_out_of_state_level_is_not_high,
    supervision_or_supervision_out_of_state_past_half_full_term_release_date,
)
from recidiviz.task_eligibility.criteria.state_specific.us_mi import (
    no_active_ppo,
    no_conditions_blocking_early_discharge,
    no_new_ineligible_offenses_for_early_discharge_from_supervision,
    no_owi_violation_on_parole_dual_supervision,
    no_pending_detainer,
    not_serving_ineligible_offenses_for_early_discharge_from_parole_dual_supervision,
    parole_dual_supervision_past_early_discharge_date,
    supervision_is_not_ic_in,
    supervision_level_is_not_modified,
    supervision_or_supervision_out_of_state_level_is_not_sai,
)
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_MI,
    task_name="COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_DUAL_SUPERVISION_REQUEST",
    description=__doc__,
    candidate_population_view_builder=parole_dual_active_supervision_and_supervision_out_of_state_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
        parole_dual_supervision_past_early_discharge_date.VIEW_BUILDER,
        supervision_not_past_full_term_completion_date_or_upcoming_30_days.VIEW_BUILDER,
        serving_at_least_one_year_on_parole_supervision_or_supervision_out_of_state.VIEW_BUILDER,
        not_serving_ineligible_offenses_for_early_discharge_from_parole_dual_supervision.VIEW_BUILDER,
        no_new_ineligible_offenses_for_early_discharge_from_supervision.VIEW_BUILDER,
        no_active_ppo.VIEW_BUILDER,
        no_owi_violation_on_parole_dual_supervision.VIEW_BUILDER,
        no_pending_detainer.VIEW_BUILDER,
        supervision_is_not_ic_in.VIEW_BUILDER,
        supervision_or_supervision_out_of_state_level_is_not_sai.VIEW_BUILDER,
        supervision_or_supervision_out_of_state_level_is_not_high.VIEW_BUILDER,
        custodial_authority_is_supervision_authority_or_other_state.VIEW_BUILDER,
        supervision_level_is_not_modified.VIEW_BUILDER,
        no_conditions_blocking_early_discharge.VIEW_BUILDER,
    ],
    completion_event_builder=early_discharge.VIEW_BUILDER,
    # Clients are almost eligible for early discharge from parole if they are within 30 days from being fully eligible,
    # which means both the parole dual supervision early discharge date AND the half full term release date
    # must be at most 30 days away.
    almost_eligible_condition=PickNCompositeCriteriaCondition(
        sub_conditions_list=[
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=parole_dual_supervision_past_early_discharge_date.VIEW_BUILDER,
                        reasons_date_field="early_discharge_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 30 days of completing mandatory period of parole",
                    ),
                    EligibleCriteriaCondition(
                        criteria=parole_dual_supervision_past_early_discharge_date.VIEW_BUILDER,
                        description="Completed mandatory period of parole",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
                        reasons_date_field="half_full_term_release_date",
                        interval_length=30,
                        interval_date_part=BigQueryDateInterval.DAY,
                        description="Within 30 days of completing half their full term supervision or supervision out of state sentence",
                    ),
                    EligibleCriteriaCondition(
                        criteria=supervision_or_supervision_out_of_state_past_half_full_term_release_date.VIEW_BUILDER,
                        description="Completed half their full term supervision or supervision out of state sentence",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
        ],
        at_least_n_conditions_true=2,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
