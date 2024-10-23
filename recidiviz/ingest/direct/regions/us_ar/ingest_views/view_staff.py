# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT 
  PARTYID,
  PERSONLASTNAME,
  PERSONFIRSTNAME,
  PERSONMIDDLENAME,
  PERSONSUFFIX,
  PARTYEMAILADDR
FROM {PERSONPROFILE}
INNER JOIN {RELATEDPARTY}
USING(PARTYID)
LEFT JOIN {PARTYPROFILE}
USING(PARTYID)
WHERE 
  REGEXP_CONTAINS(PARTYID, r'^[[:alnum:]]+$') AND
  /*
  Only ingest data from PERSONPROFILE if the person has had a role within ADC/ACC, since 
  PERSONPROFILE includes many people in external positions who don't need to be ingested. 
  We use SELECT DISTINCT in this view because people could have multiple rows of staff-type 
  relationships in RELATEDPARTY, resulting in data getting duplicated in the join.

  This set of allowed relationships may need to be expanded over time if we end up wanting
  data for people in other roles (such as court figures or external programming providers).
  */
  PARTYRELTYPE IN (
    '1AA', -- ADC Employee
    '1AB', -- ADC Employee/Extra Help
    '1AC', -- ACSD CTE
    '1AE', -- DOC Employee
    '1AI', -- IFI
    '1AP', -- PIE
    '1BB', -- Board of Corrections
    '1CA', -- Private Prison Staff	
    '2AA', -- ACC Employee
    '2BB', -- Parole Board
    '3BO', -- City Jail Employee
    '3BP', -- Police Department Employee
    '3BS', -- Sheriff Department Employee
    '3BT' -- County Jail Employee
  )
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar", ingest_view_name="staff", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
