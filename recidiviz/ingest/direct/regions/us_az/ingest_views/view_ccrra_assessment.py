# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
""" Query for Community Supervision Risk Release Assessment results."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  RISK_ASSESSMENT_ID,
  PERSON_ID,  
  CAST(DATE_ASSESSMENT AS DATETIME) AS DATE_ASSESSMENT,
  TOTAL_POINTS,
  level_lookup.DESCRIPTION AS LEVEL,
FROM
    {AZ_CS_OMS_RSK_ASSESSMENT} rsk
JOIN
    {DPP_EPISODE} dpp
USING
    (DPP_ID)
LEFT JOIN
    {LOOKUPS} level_lookup
ON
    (rsk.LEVEL_ID = level_lookup.LOOKUP_ID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="ccrra_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
