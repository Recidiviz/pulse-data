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
# MERCHANTABILITY or FIIXESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Sessions for incarceration staff caseloads in Idaho"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_ix_incarceration_staff_assignment_sessions_preprocessed"
)

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Sessions for incarceration staff caseloads in Idaho"""
)

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
#TODO(#40129): Ingest these mappings
WITH 
assignments AS (
    SELECT
      OffenderID,
      DATE(FromDate) AS FromDate,
      DATE(ToDate) AS ToDate,
      -- Keep these around so we can use it for prioritization later, even though rows
      -- will be broken up into sub-sessions
      DATE(FromDate) AS original_FromDate,
      DATE(ToDate) AS original_ToDate,
      EmployeeId
    FROM `{{project_id}}.us_ix_raw_data_up_to_date_views.hsn_CounselorAssignment_latest` 
    LEFT JOIN `{{project_id}}.us_ix_raw_data_up_to_date_views.hsn_CounselorAssignmentType_latest` 
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
    "US_IX" AS state_code,
    pei.person_id,
    pei.external_id AS person_external_id,
    FromDate AS start_date,
    ToDate AS end_date_exclusive,
    sei.staff_id AS incarceration_staff_assignment_id,
    EmployeeId AS incarceration_staff_assignment_external_id,
    "INCARCERATION_STAFF" AS incarceration_staff_assignment_role_type,
    --everyone in this view is by necessity a case manager and so should have "COUNSELOR" role subtype
    "COUNSELOR" AS incarceration_staff_assignment_role_subtype,
    ROW_NUMBER() OVER (
        PARTITION BY OffenderID, FromDate, ToDate 
        -- Give priority to assignments that started later and for those that started on
        -- the same day, the one that ended sooner (given pattern in ID we've seen where
        -- old assignments are kept open erroneously after new assignments have been 
        -- added).
        ORDER BY original_FromDate DESC, {nonnull_end_date_clause("original_FromDate")}
    ) AS case_priority
FROM sub_sessions_with_attributes
LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
    ON pei.external_id = OffenderID
    AND pei.id_type = 'US_IX_DOC'
LEFT JOIN `{{project_id}}.normalized_state.state_staff_external_id` sei
    ON EmployeeId = sei.external_id
    AND sei.state_code = "US_IX"
    AND sei.id_type = 'US_IX_EMPLOYEE'
"""

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
