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
""" Query to gather Community General Risk Levels and Community Violence Risk Levels
from the DOC Priority Report. These two scores always appear together in raw data - there
are no instances of one being hydrated where the other is not."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT
    PERSON_ID, 
    -- These scores are prefixed with a letter to denote the score category (General or Violence).
    -- Since we will store them as discrete assessments and want to know only the numerical
    -- score, we strip those prefixes here.
    REPLACE(CMNT_GNRL_RSK_LVL,'G','') AS CMNT_GNRL_RSK_LVL,
    REPLACE(CMNT_VLNC_RSK_LVE,'V','') AS CMNT_VLNC_RSK_LVE,
    DATE_CREATED
FROM 
    {DOC_PRIORITY_REPORT}
JOIN
    {DOC_EPISODE}
USING
    (DOC_ID)
-- Filter to rows that have some information in these fields. If one field is hydrated,
-- both are, but we check for either in case that ever changes.
WHERE 
    CMNT_GNRL_RSK_LVL IS NOT NULL 
    OR CMNT_VLNC_RSK_LVE IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="community_risk_level_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
