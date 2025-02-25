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
agents. See `us_ca.md` for more information about how this query works.

Supervision agents with multiple badge numbers are included multiple times (once with 
each identifying badge number). Agents with null badge numbers are currently excluded.

Things to consider making tickets for:
1. In progress question about what it means for an agent to have a null badge number.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH staff_from_AgentParole AS (
  -- `staff_from_AgentParole` selects agents from AgentParole, prioritizing names from
  -- the most recent transfers.
  SELECT
    DISTINCT BadgeNumber,
    UPPER(TRIM(SPLIT(ParoleAgentName, '|')[OFFSET(0)])) AS LastName,
    UPPER(TRIM(SPLIT(ParoleAgentName, '|')[OFFSET(1)])) AS FirstName,
  FROM {AgentParole@ALL}
  WHERE BadgeNumber IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber ORDER BY update_datetime DESC) = 1
),
staff_from_PersonParole AS (
  -- `staff_from_PersonParole` selects agents from PersonParole, prioritizing names from
  -- the most recent transfers.
  SELECT
    DISTINCT BadgeNumber,
    UPPER(TRIM(SPLIT(ParoleAgentName, ',')[OFFSET(0)])) AS LastName,
    UPPER(TRIM(SPLIT(ParoleAgentName, ',')[OFFSET(1)])) AS FirstName,
  FROM {PersonParole@ALL}
  WHERE BadgeNumber IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber ORDER BY update_datetime DESC) = 1
),
unioned AS (
  -- `unioned` Combines agents found in AgentParole and PersonParole. Later we'll
  -- prioritize AgentParole if we see duplicates.
  SELECT
    BadgeNumber,
    LastName,
    FirstName,
    0 AS sourceTablePriority
  FROM staff_from_AgentParole

  UNION ALL

  SELECT
    BadgeNumber,
    LastName,
    FirstName,
    1 AS sourceTablePriority
  FROM staff_from_PersonParole
), prioritized AS (
  -- `prioritized` ensures that we select information from AgentParole over PersonParole
  -- if there is different information for the same BadgeNumber. Also, it drops any
  -- staff who aren't parole agents -- we do this by dropping any staff whose
  -- BadgeNumber is not a sequence of 4 digits.
  SELECT
    BadgeNumber,
    LastName,
    FirstName
  FROM unioned
  -- I know I know but string formatter doesn't like [0-9]BRACE4BRACE. TODO(#30821)
  WHERE REGEXP_CONTAINS(BadgeNumber, "^[0-9][0-9][0-9][0-9]$")
  QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber ORDER BY sourceTablePriority) = 1
), add_emails AS (
  -- `add_emails` joins back to AgentParole to get emails by BadgeNumber.
  SELECT
    BadgeNumber,
    LastName,
    FirstName,
    ap.EMAILADDRESS
  FROM prioritized
  LEFT JOIN (
    SELECT BadgeNumber, EMAILADDRESS 
    FROM {AgentParole@ALL} 
    WHERE TRUE -- Required because of https://github.com/google/zetasql/issues/124
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber ORDER BY update_datetime DESC) = 1
  ) ap USING (BadgeNumber)
)
SELECT * FROM add_emails;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca", ingest_view_name="staff", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
