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
"""
Uses VantagePointRecommendations and OffenderTreatment to generate ProgramAssignments
view for the purpose of detecting Supervision Outliers.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH 
    -- Collect relevant columns from VantagePointRecommendations
    -- and creates a unique SeqNum for each person's assessments
    vantage_recs AS (
        SELECT DISTINCT 
            OffenderID,
            AssessmentID,
            Recommendation,
            CAST(null AS DATETIME) AS StartDate,
            CAST(null AS DATETIME) AS EndDate,
            CAST(RecommendationDate AS STRING) AS RecommendationDate,
            CAST(null AS STRING) AS TreatmentTerminatedReason,
            ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY AssessmentID, Recommendation, Pathway, RecommendationDate) AS SeqNum
        FROM {VantagePointRecommendations}
    )
    SELECT DISTINCT -- This concats AssessmentID and SeqNum to create a unique ID
            OffenderID,
            CONCAT(AssessmentID, '-', SeqNum) AS AssessmentID,
            CAST(StartDate AS DATETIME) AS StartDate,
            CAST(EndDate AS DATETIME) AS EndDate,
            Recommendation,
            RecommendationDate,
            TreatmentTerminatedReason
    FROM vantage_recs

    UNION ALL

    SELECT DISTINCT -- This concats program and sequence number to create a unique ID
            OffenderID,
            CONCAT(Program,'-', SequenceNumber) AS AssessmentID,
            CAST(StartDate AS DATETIME) AS StartDate,
            CAST(EndDate AS DATETIME) AS EndDate,
            CAST(null AS STRING) AS Recommendation,
            CAST(null AS STRING) AS RecommendationDate,
            TreatmentTerminatedReason
    FROM {OffenderTreatment}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="ProgramAssignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
