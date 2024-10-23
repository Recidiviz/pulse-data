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
"""Builder for a task eligibility spans view that shows the spans of time when someone
in TN is eligible for Suspension of Direct Supervision (SDS). NB: this is for SDS
specifically (the parole version of release from active supervision in TN) and not for
the parallel probation version, Judicial Suspension of Direct Supervision (JSS).
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_no_contact_parole,
)
from recidiviz.task_eligibility.criteria.general import (
    at_least_12_months_since_most_recent_positive_drug_test,
    latest_drug_test_is_negative,
    no_supervision_violation_within_2_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_sanctions_in_past_year,
    no_warrant_within_2_years,
    not_on_life_sentence_or_lifetime_supervision,
    special_conditions_are_current,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Shows the spans of time when someone in TN is eligible for Suspension
of Direct Supervision (SDS). NB: this is for SDS specifically (the parole version of
release from active supervision in TN) and not for the parallel probation version,
Judicial Suspension of Direct Supervision (JSS).
"""

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="SUSPENSION_OF_DIRECT_SUPERVISION",
    description=_DESCRIPTION,
    # TODO(#33650): Ensure that this is the correct candidate population for SDS
    # (especially with respect to ISC cases).
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        # TODO(#33627): Create additional criterion to assess whether individuals have
        # been on supervision for two years (which includes time on CR, unless they were
        # removed from CR due to a sanction). Probably needs to be state-specific
        # because of this nuance?
        # TODO(#33632): Create additional criterion to assess whether individuals have
        # an overall risk score of "minimum" during two-year period. (Does this need to
        # be state-specific, or can it be general?)
        # TODO(#33636): Create additional criterion to assess whether individuals are
        # meeting financial obligations as outlined in payment plan. May be able to
        # reuse existing existing criterion/criteria, or may need to create a new
        # criterion.
        # TODO(#33634): Double-check that this general criterion for drug tests does
        # what we need it to do for SDS! (It should be good.)
        at_least_12_months_since_most_recent_positive_drug_test.VIEW_BUILDER,
        # TODO(#33640): Double-check that this existing general criterion is correct for
        # the specific SDS requirement related to an individual's latest drug screen.
        latest_drug_test_is_negative.VIEW_BUILDER,
        no_supervision_violation_within_2_years.VIEW_BUILDER,
        # TODO(#33633): Finalize how we want to implement this criterion. Can we reuse
        # the high-sanctions criterion instead, or are "high sanctions" distinct from
        # sanctions? Could this new criterion (and/or the existing high-sanctions
        # criterion) be state-agnostic instead, if TN supervision violations and
        # sanctions have been ingested?
        no_sanctions_in_past_year.VIEW_BUILDER,
        no_warrant_within_2_years.VIEW_BUILDER,
        # TODO(#33641): Double-check that this existing state-specific criterion is
        # correct for the specific SDS requirement that individuals not be supervised
        # under a Community Supervision for Life certificate.
        not_on_life_sentence_or_lifetime_supervision.VIEW_BUILDER,
        # TODO(#33635): Double-check that this existing state-specific criterion is
        # correct for the specific SDS requirement that individuals must have completed
        # and/or complied with all special conditions.
        special_conditions_are_current.VIEW_BUILDER,
    ],
    completion_event_builder=transfer_to_no_contact_parole.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
