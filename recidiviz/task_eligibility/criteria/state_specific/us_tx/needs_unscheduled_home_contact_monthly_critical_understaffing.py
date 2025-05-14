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
"""
Defines a criteria span view that shows spans of time during which a client
with monthly unscheduled home contacts who is associated with a critically
understaffed location is due for a home contact. These monthly contacts
can be alternated between virtual and in-person visits according to a schedule
by month and client last name, as indicated by the `override_contact_type`.
"""

from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    needs_unscheduled_home_contact_standard_policy,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tx_query_fragments import (
    contact_compliance_builder_critical_understaffing_monthly_virtual_override,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which a client
with monthly unscheduled home contacts who is associated with a critically
understaffed location is due for a home contact. These monthly contacts
can be alternated between virtual and in-person visits according to a schedule
by month and client last name, as indicated by the `override_contact_type`.
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    contact_compliance_builder_critical_understaffing_monthly_virtual_override(
        description=_DESCRIPTION,
        base_criteria=needs_unscheduled_home_contact_standard_policy.VIEW_BUILDER,
        contact_type="UNSCHEDULED HOME",
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
