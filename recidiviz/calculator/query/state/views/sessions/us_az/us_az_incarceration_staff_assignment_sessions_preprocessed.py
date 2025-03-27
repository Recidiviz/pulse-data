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
"""Sessions for incarceration staff caseloads in AZ"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_az_incarceration_staff_assignment_sessions_preprocessed"
)

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Sessions for incarceration staff caseloads in AZ"""
)

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = """
#TODO(#32753): Ingest these mappings
WITH 
officer_assignments AS (
    SELECT 
        INMATE_PERSON_ID,
        AGENT_PERSON_ID,
        DATE(CAST(DATE_ASSIGNED AS DATETIME)) AS DATE_ASSIGNED,
        DATE(CAST(DATE_DEASSIGNED AS DATETIME)) AS DATE_DEASSIGNED,
        CAST(UPDT_DTM AS DATETIME) AS UPDT_DTM,
        ACTIVE_FLAG,
        LEAD(
            DATE(CAST(DATE_ASSIGNED AS DATETIME))
        ) OVER (
            PARTITION BY INMATE_ASSIGNMENT_ID, INMATE_PERSON_ID 
            ORDER BY CAST(DATE_ASSIGNED AS DATETIME), CAST(UPDT_DTM AS DATETIME)
        ) AS next_DATE_ASSIGNED
    FROM `{project_id}.{us_az_raw_data_up_to_date_dataset}.AZ_CM_INMATE_ASSIGNMENT_latest`
),
officer_assignments_with_corrected_deassign_date AS (
    SELECT 
        INMATE_PERSON_ID, AGENT_PERSON_ID,
        DATE_ASSIGNED AS start_date, 
        CASE
            -- The DATE_DEASSIGNED field is hydrated for assignments that started before the
            -- AZ data migration of Nov 2019. Post 2019-11-29 this field is always null.
            WHEN DATE_DEASSIGNED IS NOT NULL THEN DATE_DEASSIGNED
            -- For newer, active assignments, we assume the previous assignment is ended
            -- when the row is updated with a new assignment
            WHEN next_DATE_ASSIGNED IS NOT NULL THEN next_DATE_ASSIGNED
            -- For inactive assignments, we must use the update datetime on the assignment
            -- row to determine when the assignment ends.
            WHEN ACTIVE_FLAG = 'N' THEN DATE(UPDT_DTM)
            ELSE NULL
        END AS end_date
    FROM officer_assignments
),
filtered_officer_assignments AS (
    SELECT INMATE_PERSON_ID, AGENT_PERSON_ID, start_date, end_date
    FROM officer_assignments_with_corrected_deassign_date
    WHERE
        -- Filter out zero-day assignments 
        end_date IS NULL OR start_date != end_date
)
SELECT
    "US_AZ" AS state_code,
    pei.person_id AS person_id,
    INMATE_PERSON_ID AS person_external_id,
    -- Cast back to DATETIME so types agree with other state-specific pre-processing 
    -- views.
    DATETIME(start_date) AS start_date,
    -- Cast back to DATETIME so types agree with other state-specific pre-processing 
    -- views.
    DATETIME(end_date) AS end_date_exclusive,
    sei.staff_id AS incarceration_staff_assignment_id,
    AGENT_PERSON_ID AS incarceration_staff_assignment_external_id,
    'INCARCERATION_STAFF' AS incarceration_staff_assignment_role_type,
    'COUNSELOR' AS incarceration_staff_assignment_role_subtype,
    -- We don't expect any overlaps so all case_priority are 1
    1 AS case_priority
FROM filtered_officer_assignments
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
ON(INMATE_PERSON_ID = pei.external_id AND pei.id_type = 'US_AZ_PERSON_ID')
INNER JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` sei
ON(AGENT_PERSON_ID = sei.external_id AND sei.id_type = 'US_AZ_PERSON_ID')
"""

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
