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
"""Query containing TN Violation and Sanction information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
  sanctions_groupings AS (
  SELECT 
    TriggerNumber,
    TO_JSON_STRING(
                ARRAY_AGG(STRUCT<OffenderID string,
                                TriggerNumber string,
                                Id string,
                                ProposedSanction string,
                                SanctionLevel string,
                                SanctionStatus string,
                                ImposedDate string>
                        (OffenderID,
                        TriggerNumber,
                        Id,
                        ProposedSanction,
                        SanctionLevel,
                        SanctionStatus,
                        ImposedDate) ORDER BY TriggerNumber, Id)
            ) as sanction_info
  FROM {Sanctions}
  LEFT JOIN {Violations} USING (TriggerNumber)
  GROUP BY TriggerNumber
)
SELECT
  OffenderID,
  TriggerNumber,
  ContactNoteDate,
  sanction_info, 
  ContactNoteType
FROM {Violations}
LEFT JOIN sanctions_groupings USING (TriggerNumber)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="ViolationsAndSanctions",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID, ContactNoteDate",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
