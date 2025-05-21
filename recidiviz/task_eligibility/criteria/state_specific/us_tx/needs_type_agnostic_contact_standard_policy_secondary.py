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

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tx_query_fragments import (
    contact_compliance_builder_type_agnostic,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NEEDS_TYPE_AGNOSTIC_CONTACT_STANDARD_POLICY_SECONDARY"

_WHERE_CLAUSE = """
        -- This includes the second type-agnostic criteria for any case type and supervision 
        -- level combinations that have two different type-agnostic contact requirements
        WHERE (
            supervision_level = 'MAXIMUM'
            AND case_type = 'GENERAL'
            AND contact_types_accepted = 'UNSCHEDULED FIELD,UNSCHEDULED HOME,' 
            AND frequency_in_months = '3'
        )
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    contact_compliance_builder_type_agnostic(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        where_clause=_WHERE_CLAUSE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
