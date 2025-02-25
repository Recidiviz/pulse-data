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
"""Query containing CAFScore assessment information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH latest_Classification AS (
  SELECT 
    t.OffenderID, 
    ClassificationDate, 
    ClassificationDecisionDate, 
    (DATE(CAFDate)) AS CAFDate, 
    OverrideReason, 
    ClassificationDecision
  FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY OffenderId, CAFDate ORDER BY ClassificationSequenceNumber DESC) AS seq 
        FROM {Classification}) t
  WHERE seq = 1
), CAF_base AS (
    SELECT 
        ca.OffenderID,
        ca.CAFDate,
        (DATE(ClassificationDate)) AS ClassificationDate,
        (DATE(ClassificationDecisionDate)) AS ClassificationDecisionDate,
        CAFScore,
        OverrideReason,
        ClassificationDecision,
        ScheduleAScore,
        ScheduleBScore,
        CAFCustodyLevel,
        CategoryScore1 AS Question1,
        CategoryScore2 AS Question2,
        CategoryScore3 AS Question3,
        CategoryScore4 AS Question4,
        CategoryScore5 AS Question5,
        CategoryScore7 AS Question6,
        CategoryScore8 AS Question7,
        CategoryScore9 AS Question8,
        CategoryScore10 AS Question9,
        CAFSiteID, 
        ROW_NUMBER() OVER (PARTITION BY ca.OffenderID ORDER BY ca.CAFDate) AS CAF_ID,
    FROM {CAFScore} ca
    LEFT JOIN latest_Classification cl
    ON ca.OffenderId = cl.OffenderId
    AND (DATE(ca.CAFDate)) = cl.CAFDate
)
SELECT * FROM CAF_base
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="CAFScoreAssessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
