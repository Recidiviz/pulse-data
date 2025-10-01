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
"""Defines a criteria view that shows spans of time for which supervision clients
do not meet standards for type agnostic contacts, specifically for case type and
supervision level combinations that have two different type-agnostic contact requirements.
Criteria may only have one span per time period per person, so clients with two
type-agnostic contact requirements must be separated into a second criteria to allow
for two overlapping spans.
Currently, this only applies to MAXIMUM (High in TX) GENERAL (Regular in TX) clients.
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    prioritized_supervision_population_with_officer,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    needs_type_agnostic_contact_standard_policy_secondary,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_TX,
    task_name="needs_type_agnostic_contact_standard_policy_secondary",
    candidate_population_view_builder=prioritized_supervision_population_with_officer.VIEW_BUILDER,
    criteria_spans_view_builders=[
        needs_type_agnostic_contact_standard_policy_secondary.VIEW_BUILDER,
    ],
    compliance_type=ComplianceType.CONTACT,
    due_date_field="contact_due_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
