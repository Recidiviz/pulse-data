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
"""Query containing person information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH employer_address AS (
  SELECT DISTINCT
    EMPLOYER,
    STREETNUMBER,
    STREETNAME,
    STREETTYPE,
    SUITENUMBER,
    APARTMENTNUM,
    POBOX,
    CITY,
    STATE,
    ZIPCODE
  FROM (
    SELECT 
      *,
      RANK() OVER (
        PARTITION BY EMPLOYER 
        ORDER BY 
          rp.DATELASTUPDATE DESC, 
          rp.TIMELASTUPDATE DESC, 
          a.DATELASTUPDATE DESC, 
          a.TIMELASTUPDATE DESC,
          a.ADDRESSID
      ) AS address_rank
    FROM {EMPLOYMENTHISTORY} eh
    LEFT JOIN {RELATEDPARTY} rp ON eh.EMPLOYER = rp.PARTYID AND 
      rp.PARTYRELTYPE = '9AA' AND 
      rp.PRIMARYSITEIND = 'Y' AND
      rp.PARTYRELSTATUS = 'A'
    LEFT JOIN {ADDRESS} a ON a.ADDRESSID = rp.RELATEDPARTYID
  ) ranked_addresses
  WHERE address_rank = 1
)

SELECT 
  OFFENDERID, 
  EMPSTARTDATE,
  NULLIF(EMPLENDDATE,'9999-12-31 00:00:00') AS EMPLENDDATE, 
  NULLIF(VERIFIEDDATE,'1000-01-01 00:00:00') AS VERIFIEDDATE,
  NATUREOFJOB,
  REASONFORLEAVING,
  OFFNISUNEMPLOYED,
  OFFNISDISABLED,
  OCCUPATIONCODE,
  op.ORGANIZATIONNAME AS EMPLOYERNAME,
  ea.STREETNUMBER,
  ea.STREETNAME,
  ea.STREETTYPE,
  ea.SUITENUMBER,
  ea.APARTMENTNUM,
  ea.POBOX,
  ea.CITY,
  ea.STATE,
  ea.ZIPCODE,
  ROW_NUMBER() OVER (PARTITION BY OFFENDERID ORDER BY EMPSTARTDATE) AS SEQ
FROM {EMPLOYMENTHISTORY} eh
LEFT JOIN employer_address ea USING(EMPLOYER)
LEFT JOIN {ORGANIZATIONPROF} op ON eh.EMPLOYER = op.PARTYID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="employment_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
