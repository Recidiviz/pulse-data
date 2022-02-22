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
"""This query pulls from the `ContactNoteType` table provided by TN and lists (when applicable) the corresponding
StaffID that entered the contact from `Stqff` table. The table contains one row per contact, and this query takes
each contact and ultimately maps it into SupervisionContacts for a person.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH contact_note_type_view as ( 
    SELECT 
        ContactNotes.OffenderID, 
        ContactNotes.ContactNoteDateTime,
        ContactNotes.ContactNoteType,
        Staff.StaffID,
    FROM {ContactNoteType} AS ContactNotes
    LEFT JOIN {Staff} AS Staff on Staff.UserID = ContactNotes.LastUpdateUserID
)
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY ContactNoteDateTime ASC) AS ContactSequenceNumber
FROM contact_note_type_view
WHERE CAST(ContactNoteDateTime AS DATETIME) >= CAST('1997-01-01' AS DATETIME)
AND CAST(ContactNoteDateTime AS DATETIME) < CAST('2000-01-01' AS DATETIME)
"""

# TODO(#11242): Remove datetime filters and remerge back to one SupervisionContacts
# once scale issues are resolved.
VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="SupervisionContacts1997to2000",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
