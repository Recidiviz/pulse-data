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
"""Task eligibility spans view that shows the spans of time when someone in TN is
eligible for Suspension of Direct Supervision (SDS). NB: this is for SDS specifically
(the parole version of release from active supervision in TN) and not for the parallel
probation version, Judicial Suspension of Direct Supervision (JSS)."""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_population,
)
from recidiviz.task_eligibility.completion_events.state_specific.us_tn import (
    transfer_to_no_contact_parole,
)
from recidiviz.task_eligibility.criteria.general import (
    assessed_risk_low_at_least_2_years,
    at_least_12_months_since_most_recent_positive_drug_test,
    latest_drug_test_is_negative,
    no_supervision_violation_report_within_2_years,
    on_supervision_at_least_2_years,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn import (
    no_supervision_sanction_within_1_year,
    no_warrant_within_2_years,
    not_interstate_compact_incoming,
    not_on_life_sentence_or_lifetime_supervision,
    special_conditions_are_current,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    task_name="SUSPENSION_OF_DIRECT_SUPERVISION",
    description=__doc__,
    # TODO(#34432): Since ISC-out cases would be eligible for SDS, we probably want to
    # update the candidate population to include `SUPERVISION_OUT_OF_STATE` clients
    # (which may require the creation of a new candidate population).
    candidate_population_view_builder=parole_active_supervision_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        assessed_risk_low_at_least_2_years.VIEW_BUILDER,
        # TODO(#33636): Create additional criterion to assess whether individuals are
        # meeting financial obligations as outlined in payment plan. May be able to
        # reuse existing existing criterion/criteria, or may need to create a new
        # criterion.
        at_least_12_months_since_most_recent_positive_drug_test.VIEW_BUILDER,
        # TODO(#33640): Double-check that this existing general criterion is correct for
        # the specific SDS requirement related to an individual's latest drug screen.
        latest_drug_test_is_negative.VIEW_BUILDER,
        no_supervision_violation_report_within_2_years.VIEW_BUILDER,
        # TODO(#33627): Update/change time-served criterion to account for the fact that
        # if individuals are "removed from compliant reporting due to the imposition of
        # a sanction," then that time on compliant reporting does not count towards the
        # two-years-on-supervision requirement for SDS. May need to shift to using a
        # state-specific criterion, then? (If so, check that nobody has starting using
        # the general criterion before deleting it.)
        on_supervision_at_least_2_years.VIEW_BUILDER,
        no_supervision_sanction_within_1_year.VIEW_BUILDER,
        no_warrant_within_2_years.VIEW_BUILDER,
        not_interstate_compact_incoming.VIEW_BUILDER,
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
