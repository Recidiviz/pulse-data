# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing CDCR staff member information. Currently only looks at supervision
agents.

Supervision agents with multiple badge numbers are included multiple times (once with 
each identifying badge number). Agents with null badge numbers are currently excluded.

Tickets to improve this view:
1. #TODO(#21323): Create a validation view to track officers with multiple badge numbers.

Things to consider making tickets for:
1. In progress question about what it means for an agent to have a null badge number.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
  SELECT
    DISTINCT BadgeNumber,
    TRIM(SPLIT(ParoleAgentName, '|')[OFFSET(0)]) as LastName,
    TRIM(SPLIT(ParoleAgentName, '|')[OFFSET(1)]) as FirstName,
  FROM {AgentParole}
  WHERE BadgeNumber IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BadgeNumber",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
