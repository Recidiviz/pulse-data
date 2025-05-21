# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates the state supervision contacts entity using the following tables: """
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    SID_Number,
    Contact_ID,
    Contact_Date,
    Verified_Employment,
    Face_to_Face_Flag,
    Unsuccessful_Contact_Flag,
    Reason_Description,
    Location_Description,
    Type_Description,
    COLLATERAL_CONTACT,
FROM {SupervisionContact}
LEFT JOIN {ContactTypeDescription}
    USING(Contact_Type)
LEFT JOIN {ContactReasonDescription}
    USING(Contact_Reason)
LEFT JOIN {ContactLocationDescription}
    USING(Contact_Location)
-- These flags mean the contact is not deleted
WHERE UPPER(Deleted_Flag) IN ("ACTIVE", "0") 
AND UPPER(Reason_Description) NOT IN ("ALERT NOTIFICATION - EQUIPMENT")
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="supervision_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
