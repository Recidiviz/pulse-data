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
Query to hydrate the StatePersonStaffRelationshipPeriod entities in US_IX.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- Prep assignments for sub-sessions
assignments AS (
    SELECT
      OffenderID,
      DATE(FromDate) AS FromDate,
      DATE(ToDate) AS ToDate,
      -- Keep these around so we can use it for prioritization later, even though rows
      -- will be broken up into sub-sessions
      DATE(FromDate) AS original_FromDate,
      DATE(ToDate) AS original_ToDate,
      EmployeeId,
      CONCAT("ATLAS-", LocationId) AS location_external_id
    FROM {{hsn_CounselorAssignment}}
    LEFT JOIN {{hsn_CounselorAssignmentType}}
        USING (CounselorAssignmentTypeId)
    WHERE
        offenderId IS NOT NULL
        -- Filter out zero-day periods
        AND (ToDate IS NULL OR DATE(FromDate) < DATE(ToDate))
        --only include primary case manager <> resident mappings
        AND CounselorAssignmentTypeName = "Primary" 
    -- We see a regular pattern in ID of two assignment rows getting created on the same
    -- day, for the same case manager, with one of those rows getting closed out 
    -- properly when the new row is added / the case manager is changed, but one of
    -- those rows staying open for a much longer time. We attempt here to drop all those
    -- rows that were opened erroneously and kept open too long.  
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY OffenderId, FromDate, EmployeeId 
        ORDER BY DATE({nonnull_end_date_clause('ToDate')})
    ) = 1
),
-- Sub-sessionize to allow us to prioritize any overlapping periods
{create_sub_sessions_with_attributes(table_name ='assignments',
                                     start_date_field_name="FromDate",
                                     end_date_field_name='ToDate',
                                     index_columns=['OffenderID'])}
SELECT
    OffenderID,
    FromDate,
    ToDate,
    EmployeeId,
    location_external_id,
    ROW_NUMBER() OVER (
        PARTITION BY OffenderID, FromDate, ToDate 
        -- Give priority to assignments that started later and for those that started on
        -- the same day, the one that ended sooner (given pattern in ID we've seen where
        -- old assignments are kept open erroneously after new assignments have been 
        -- added).
        ORDER BY original_FromDate DESC, {nonnull_end_date_clause("original_FromDate")}
    ) AS relationship_priority
FROM sub_sessions_with_attributes
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="incarceration_person_staff_relationship_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
