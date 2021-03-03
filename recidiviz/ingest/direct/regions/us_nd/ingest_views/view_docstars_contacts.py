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
"""Query containing contacts information.

   TODO(#5057): Update description once we ingest contact codes 2 - 6.
   NOTE: This is only capturing contacts logged by the supervising officer."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH contacts_with_split_supervisor_name AS (
  SELECT
    RecID,
    SID,
    CONTACT_DATE,
    CONTACT_CODE,
    C2 AS CONTACT_CODE_2,
    C3 AS CONTACT_CODE_3,
    C4 AS CONTACT_CODE_4,
    C5 AS CONTACT_CODE_5,
    C6 AS CONTACT_CODE_6,
    CATEGORY,
    SPLIT(ORIGINATOR, ', ')[OFFSET(0)] LNAME,
    SPLIT(ORIGINATOR, ', ')[OFFSET(1)] FNAME,
  FROM {docstars_contacts}
  WHERE CONTACT_CODE IS NOT NULL
  # Exclude system generated entries, as those don't represent contacts.
  AND CONTACT_CODE != "SG"
  # ND includes supervision and FTR contacts. Only include supervision contacts
  AND CATEGORY = "Supervision"
 )
SELECT 
  RecID,
  SID,
  CONTACT_DATE,
  CONTACT_CODE,
  # TODO(#5057): Properly ingest CONTACT_CODES 2-6, especially when CONTACT_CODE is null. 
  CONTACT_CODE_2,
  CONTACT_CODE_3,
  CONTACT_CODE_4,
  CONTACT_CODE_5,
  CONTACT_CODE_6,
  CATEGORY,
  contacts_with_split_supervisor_name.LNAME,
  contacts_with_split_supervisor_name.FNAME,
  OFFICER, 
  FROM contacts_with_split_supervisor_name LEFT JOIN
  {docstars_officers} officers
  ON (
    contacts_with_split_supervisor_name.LNAME = officers.LNAME AND 
    contacts_with_split_supervisor_name.FNAME = officers.FNAME
  )
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="docstars_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="SID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
