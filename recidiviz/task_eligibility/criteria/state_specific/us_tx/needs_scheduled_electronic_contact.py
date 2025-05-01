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

"""Defines a criteria view that shows spans of time for which supervision clients
are not compliant with scheduled electronic contacts
"""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tx_query_fragments import (
    contact_compliance_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NEEDS_SCHEDULED_ELECTRONIC_CONTACT"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
do not meet standards for scheduled electronic contacts based on their supervision level and case type.
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = contact_compliance_builder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    contact_type="SCHEDULED ELECTRONIC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
