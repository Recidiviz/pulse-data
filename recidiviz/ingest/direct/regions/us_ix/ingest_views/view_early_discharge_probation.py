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
"""Query for probation early discharges using Atlas data"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Idaho has provided guidance for officers to use the com_Investigation table in Atlas
# to record probation early discharge requests, but it is unclear to them how much officers
# are following that guidance.
VIEW_QUERY_TEMPLATE = """
SELECT
  OffenderId, 
  InvestigationId,
  Accepted,
  Cancelled,
  (DATE(RequestDate)) as RequestDate,
  -- Enforce that CompletionDate is not in the future
  CASE 
    WHEN (DATE(CompletionDate)) >= CURRENT_DATETIME
        THEN NULL
    ELSE 
        DATE(CompletionDate)
  END AS CompletionDate,

FROM {com_Investigation}
WHERE InvestigationTypeId = '1000' -- Request for Early Discharge - Probation
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="early_discharge_probation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
