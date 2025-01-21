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
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_ix_incarceration_staff_assignment_sessions_preprocessed"
)

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Sessions for incarceration staff caseloads in Idaho"""
)

US_IX_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = """
    #TODO(#32752): Ingest these mappings
 SELECT
  pei.state_code,
  pei.person_id,
  pei.external_id AS person_external_id,
  DATE(FromDate) AS start_date,
  DATE(ToDate) AS end_date_exclusive,
  sei.staff_id AS incarceration_staff_assignment_id,
  EmployeeId AS incarceration_staff_assignment_external_id,
  "INCARCERATION_STAFF" AS incarceration_staff_assignment_role_type,
  --everyone in this view is by necessity a case manager and so should have "COUNSELOR" role subtype
  "COUNSELOR" AS incarceration_staff_assignment_role_subtype,
    NULL AS case_priority,
FROM `{project_id}.us_ix_raw_data_up_to_date_views.hsn_CounselorAssignment_latest` 
LEFT JOIN `{project_id}.us_ix_raw_data_up_to_date_views.hsn_CounselorAssignmentType_latest` 
    USING (CounselorAssignmentTypeId)
LEFT JOIN `{project_id}.normalized_state.state_person_external_id` pei
    ON pei.external_id = OffenderID
    AND pei.id_type = 'US_IX_DOC'
LEFT JOIN `{project_id}.normalized_state.state_staff_external_id` sei
    ON EmployeeId = sei.external_id
    AND sei.state_code = "US_IX"
    AND sei.id_type = 'US_IX_EMPLOYEE'
WHERE offenderId IS NOT NULL
    AND FromDate < ToDate
    --only include primary case manager <> resident mappings
    AND CounselorAssignmentTypeName = "Primary"

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
