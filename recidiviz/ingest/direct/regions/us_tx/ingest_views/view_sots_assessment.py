# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
This ingest view pulls in sex offender static 99 assessments (SOTS).
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    SID_Number,
    SOTS_RISK_LEVEL,
    DATE(SOTS_TEST_DATE) AS SOTS_TEST_DATE
FROM
    {AssessmentAdditions}
WHERE 
    SOTS_RISK_LEVEL IS NOT NULL
AND
    SOTS_TEST_DATE IS NOT NULL
AND
    DATE(SOTS_TEST_DATE) <= CURRENT_DATE
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="sots_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
